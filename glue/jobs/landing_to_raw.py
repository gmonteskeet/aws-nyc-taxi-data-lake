"""
Glue Job: Landing → Raw Zone
Data Quality Validation with Quarantine Logging

Improvements:
- Comprehensive validation rules
- Quarantine logging with statistics
- Clear error categorization
- Easy quarantine review via CloudWatch metrics
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from datetime import datetime

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'month', 'source'])
month = args['month']  # Format: "2024-01"
source = args['source']  # e.g., "yellow-trips"

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
BUCKET = "peruvianbucket"
year, month_num = month.split('-')

# Paths
landing_path = f"s3://{BUCKET}/landing/{source}/year={year}/month={month_num}/"
raw_path = f"s3://{BUCKET}/raw/{source}/year={year}/month={month_num}/"
quarantine_path = f"s3://{BUCKET}/quarantine/{source}/year={year}/month={month_num}/"

print(f"=" * 80)
print(f"Glue Job: Landing → Raw")
print(f"Processing: {landing_path}")
print(f"=" * 80)

try:
    # ========================================================================
    # 1. READ DATA FROM LANDING
    # ========================================================================
    df = spark.read.parquet(landing_path)
    initial_count = df.count()
    print(f"Records in landing: {initial_count:,}")
    
    # Add metadata columns for traceability
    df = df.withColumn("ingestion_timestamp", F.lit(datetime.utcnow()))
    df = df.withColumn("source_file", F.input_file_name())
    
    # ========================================================================
    # 2. SCHEMA VALIDATION
    # ========================================================================
    print("\n--- Schema Validation ---")
    required_columns = [
        'VendorID', 
        'tpep_pickup_datetime', 
        'tpep_dropoff_datetime',
        'passenger_count', 
        'trip_distance', 
        'fare_amount',
        'PULocationID',
        'DOLocationID'
    ]
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        error_msg = f"ERROR: Missing required columns: {missing_columns}"
        print(error_msg)
        
        # Write entire dataset to quarantine with error flag
        df_with_error = df.withColumn("quarantine_reason", F.lit("missing_columns"))
        df_with_error.write.mode("overwrite").parquet(quarantine_path)
        
        raise Exception(error_msg)
    
    print("✓ All required columns present")
    
    # ========================================================================
    # 3. DATA QUALITY VALIDATION RULES
    # ========================================================================
    print("\n--- Data Quality Validation ---")
    
    # Rule 1: No nulls in critical columns
    null_checks = (
        F.col("tpep_pickup_datetime").isNotNull() &
        F.col("tpep_dropoff_datetime").isNotNull() &
        F.col("fare_amount").isNotNull() &
        F.col("trip_distance").isNotNull()
    )
    
    # Rule 2: Non-negative amounts
    positive_checks = (
        (F.col("fare_amount") >= 0) &
        (F.col("trip_distance") >= 0) &
        (F.col("passenger_count") > 0)
    )
    
    # Rule 3: Temporal consistency (pickup before dropoff)
    temporal_check = (
        F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")
    )
    
    # Rule 4: Reasonable value ranges
    range_checks = (
        (F.col("passenger_count") <= 8) &  # Max 8 passengers
        (F.col("trip_distance") < 500) &   # Less than 500 miles
        (F.col("fare_amount") < 1000)      # Less than $1000
    )
    
    # Rule 5: Valid location IDs (1-263 for NYC zones)
    location_checks = (
        (F.col("PULocationID").between(1, 263)) &
        (F.col("DOLocationID").between(1, 263))
    )
    
    # Combine all validation rules
    all_checks = (
        null_checks & 
        positive_checks & 
        temporal_check & 
        range_checks & 
        location_checks
    )
    
    # ========================================================================
    # 4. SEPARATE VALID AND INVALID RECORDS
    # ========================================================================
    valid_df = df.filter(all_checks)
    invalid_df = df.subtract(valid_df)
    
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    
    # Calculate quality metrics
    quality_rate = (valid_count / initial_count * 100) if initial_count > 0 else 0
    
    print(f"\nValidation Results:")
    print(f"  Total records:   {initial_count:,}")
    print(f"  Valid records:   {valid_count:,} ({quality_rate:.2f}%)")
    print(f"  Invalid records: {invalid_count:,} ({100-quality_rate:.2f}%)")
    
    # ========================================================================
    # 5. CATEGORIZE QUARANTINE REASONS (for easy review)
    # ========================================================================
    if invalid_count > 0:
        print("\n--- Quarantine Analysis ---")
        
        # Add reason column for debugging
        invalid_with_reasons = invalid_df.withColumn(
            "quarantine_reason",
            F.when(F.col("tpep_pickup_datetime").isNull(), "null_pickup_datetime")
            .when(F.col("tpep_dropoff_datetime").isNull(), "null_dropoff_datetime")
            .when(F.col("fare_amount").isNull(), "null_fare")
            .when(F.col("trip_distance").isNull(), "null_distance")
            .when(F.col("fare_amount") < 0, "negative_fare")
            .when(F.col("trip_distance") < 0, "negative_distance")
            .when(F.col("passenger_count") <= 0, "invalid_passenger_count")
            .when(F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"), "temporal_inconsistency")
            .when(F.col("trip_distance") >= 500, "excessive_distance")
            .when(F.col("fare_amount") >= 1000, "excessive_fare")
            .when(~F.col("PULocationID").between(1, 263), "invalid_pickup_location")
            .when(~F.col("DOLocationID").between(1, 263), "invalid_dropoff_location")
            .otherwise("multiple_issues")
        )
        
        # Show quarantine breakdown
        quarantine_summary = invalid_with_reasons.groupBy("quarantine_reason").count()
        quarantine_summary.show(truncate=False)
        
        # Write to quarantine
        invalid_with_reasons.write.mode("overwrite").parquet(quarantine_path)
        print(f"✓ Quarantined {invalid_count:,} records to: {quarantine_path}")
        
        # Log warning if quality is below threshold
        if quality_rate < 95.0:
            print(f"\n  WARNING: Data quality below 95% threshold ({quality_rate:.2f}%)")
            print(f"    Review quarantine: {quarantine_path}")
    
    # ========================================================================
    # 6. WRITE VALID DATA TO RAW ZONE
    # ========================================================================
    if valid_count > 0:
        valid_df.write.mode("overwrite").parquet(raw_path)
        print(f"\n✓ Successfully wrote {valid_count:,} valid records to raw zone")
        print(f"  Location: {raw_path}")
    else:
        print("\n  WARNING: No valid records to write!")
    
    # ========================================================================
    # 7. SUMMARY STATISTICS
    # ========================================================================
    print("\n" + "=" * 80)
    print("JOB SUMMARY")
    print("=" * 80)
    print(f"Input:      {initial_count:,} records")
    print(f"Valid:      {valid_count:,} records ({quality_rate:.2f}%)")
    print(f"Quarantine: {invalid_count:,} records ({100-quality_rate:.2f}%)")
    print(f"Status:     {'✓ SUCCESS' if quality_rate >= 95 else '  WARNING - Low Quality'}")
    print("=" * 80)
    
    job.commit()
    
except Exception as e:
    print(f"\n ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    job.commit()
    raise e
