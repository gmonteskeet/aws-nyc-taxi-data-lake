"""
NYC TLC Data Pipeline - Production DAG (Manual Trigger)

Design Philosophy:
- Manual trigger only (schedule_interval=None) for full control
- Backfill via CLI commands for historical data
- Graceful handling of missing data (404s don't fail pipeline)
- Fully parameterized Lambda calls (no hardcoding in Lambda)
- Clear logging and monitoring

Usage:
    # Trigger for specific month
    airflow dags trigger nyc_tlc_pipeline --conf '{"month": "2024-01"}'
    
    # Backfill 2024 (run from CLI)
    airflow dags backfill nyc_tlc_pipeline \
        --start-date 2024-01-01 \
        --end-date 2024-12-31
"""
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import json
import logging

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Data source configuration (externalized - not in Lambda!)
NYC_TLC_CONFIG = {
    'base_url': 'https://d37ci6vzurychx.cloudfront.net/trip-data',
    'file_prefix': 'yellow_tripdata',
    'file_extension': 'parquet',
    's3_key_pattern': 'landing/yellow-trips/year={year}/month={month}/{filename}',
    'bucket_name': 'peruvianbucket',
    'source': 'yellow-trips'
}

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),  # Start from 2024
    'email': ['data-alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def check_lambda_response(**context):
    """
    Evaluate Lambda response to determine next step
    
    Returns:
        str: 'glue_landing_to_raw' if data exists, 'skip_processing' if 404
    """
    ti = context['ti']
    lambda_response = ti.xcom_pull(task_ids='lambda_ingest_to_landing')
    
    # Lambda returns dict with 'statusCode' and 'body'
    status_code = lambda_response.get('statusCode')
    body = lambda_response.get('body', {})
    
    if status_code == 404 or body.get('skip', False):
        logger.info("Data not available for this period - skipping Glue processing")
        return 'skip_processing'
    
    if status_code == 200:
        logger.info("Data successfully ingested - proceeding with ETL")
        return 'glue_landing_to_raw'
    
    # Unexpected status - log and skip
    logger.warning(f"Unexpected Lambda status {status_code} - skipping processing")
    return 'skip_processing'


# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id='nyc_tlc_pipeline',
    default_args=default_args,
    description='NYC TLC Pipeline: Manual trigger, Lambda → Glue → Structured',
    schedule_interval=None,  # MANUAL TRIGGER ONLY - no automatic scheduling
    catchup=False,  # No automatic backfill - explicit CLI backfill only
    max_active_runs=3,  # Allow 3 parallel months when backfilling
    tags=['nyc-tlc', 'data-lake'],
    doc_md=__doc__,
    params={
        'month': '2024-01',  # Default month parameter (can be overridden)
    }
) as dag:

    start = EmptyOperator(
        task_id='start',
        doc_md="Pipeline entry point"
    )
    
    # Extract month parameter
    # If triggered manually: airflow dags trigger ... --conf '{"month": "2024-05"}'
    # If backfilling: Uses execution_date to compute month
    month = "{{ dag_run.conf.get('month', execution_date.strftime('%Y-%m')) }}"
    year = "{{ dag_run.conf.get('month', execution_date.strftime('%Y-%m')).split('-')[0] }}"
    month_num = "{{ dag_run.conf.get('month', execution_date.strftime('%Y-%m')).split('-')[1] }}"
    
    # =========================================================================
    # TASK 1: Lambda Ingestion (Fully Parameterized)
    # =========================================================================
    lambda_ingest = LambdaInvokeFunctionOperator(
        task_id='lambda_ingest_to_landing',
        function_name='firstIngestor',  # Your Lambda function name
        payload=json.dumps({
            # Required parameters
            'base_url': NYC_TLC_CONFIG['base_url'],
            'file_prefix': NYC_TLC_CONFIG['file_prefix'],
            'date_param': month,
            's3_key_pattern': NYC_TLC_CONFIG['s3_key_pattern'],
            
            # Optional parameters
            'file_extension': NYC_TLC_CONFIG['file_extension'],
            'bucket_name': NYC_TLC_CONFIG['bucket_name'],
            'check_exists': True,
            
            # Parameters for S3 key construction
            'source': NYC_TLC_CONFIG['source'],
            'year': year,
            'month': month_num,
        }),
        aws_conn_id='aws_default',
        doc_md="""
        Invokes generic Lambda function with NYC TLC configuration.
        Lambda downloads data and uploads to S3 landing zone.
        Returns 404 if data not yet available.
        """
    )
    
    # =========================================================================
    # TASK 2: Branch Logic - Check Data Availability
    # =========================================================================
    check_file_exists = BranchPythonOperator(
        task_id='check_file_exists',
        python_callable=check_lambda_response,
        provide_context=True,
        doc_md="""
        Evaluates Lambda response:
        - 200: Data available → proceed to Glue processing
        - 404: Data not available → skip processing (not an error)
        - Other: Log warning and skip
        """
    )
    
    # =========================================================================
    # TASK 3: Glue ETL - Landing to Raw (Data Quality)
    # =========================================================================
    glue_landing_to_raw = GlueJobOperator(
        task_id='glue_landing_to_raw',
        job_name='landing_to_raw',
        script_args={
            '--month': month,
            '--source': 'yellow-trips',
            '--enable-metrics': '',
            '--enable-continuous-cloudwatch-log': 'true'
        },
        aws_conn_id='aws_default',
        retries=1,
        doc_md="""
        Glue job: Data quality validation
        - Schema validation
        - Null checks
        - Business rule validation
        - Quarantine invalid records (with logging)
        """
    )
    
    # =========================================================================
    # TASK 4: Glue ETL - Raw to Structured (Transformations)
    # =========================================================================
    glue_raw_to_structured = GlueJobOperator(
        task_id='glue_raw_to_structured',
        job_name='raw_to_structured',
        script_args={
            '--month': month,
            '--source': 'yellow-trips',
            '--enable-metrics': '',
            '--enable-continuous-cloudwatch-log': 'true'
        },
        aws_conn_id='aws_default',
        retries=1,
        doc_md="""
        Glue job: Transformations and feature engineering
        - Type casting
        - Feature engineering (time-based, calculated metrics)
        - Outlier removal
        - Partitioning by date (ds=YYYY-MM-DD)
        """
    )
    
    # =========================================================================
    # TASK 5: Skip Path (When Data Not Available)
    # =========================================================================
    skip_processing = EmptyOperator(
        task_id='skip_processing',
        trigger_rule=TriggerRule.NONE_FAILED,
        doc_md="Executed when data is not yet available (404 from Lambda)"
    )
    
    # =========================================================================
    # TASK 6: End
    # =========================================================================
    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="Pipeline completion"
    )
    
    # =========================================================================
    # PIPELINE FLOW
    # =========================================================================
    start >> lambda_ingest >> check_file_exists
    
    # Branch 1: Data available → Full ETL pipeline
    check_file_exists >> glue_landing_to_raw >> glue_raw_to_structured >> end
    
    # Branch 2: Data not available → Skip processing
    check_file_exists >> skip_processing >> end
