"""
Generic HTTP-to-S3 Ingestor Lambda Function

This Lambda function is designed to be FULLY REUSABLE for any HTTP data source.
All configuration is passed via event parameters - nothing is hardcoded.

Design Principle: Configuration as Parameters (externalized configuration)
- Supports any HTTP endpoint
- Flexible S3 path construction
- Configurable via caller (Airflow, Step Functions, etc.)
"""
import boto3
import requests
import os
import logging
from datetime import datetime
from urllib.parse import urlparse

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

# Only default that makes sense at Lambda level
DEFAULT_TIMEOUT = 300  # 5 minutes for large files


def check_file_exists(url, timeout=10):
    """
    Check if file exists at URL before downloading
    
    Args:
        url (str): URL to check
        timeout (int): Request timeout in seconds
        
    Returns:
        bool: True if file exists (200 status), False otherwise
    """
    try:
        response = requests.head(url, timeout=timeout, allow_redirects=True)
        return response.status_code == 200
    except requests.exceptions.RequestException as e:
        logger.warning(f"Error checking file existence: {e}")
        return False


def construct_filename(file_prefix, date_param, file_extension):
    """
    Construct filename from parameters
    
    Args:
        file_prefix (str): File prefix (e.g., "yellow_tripdata")
        date_param (str): Date parameter (e.g., "2024-01")
        file_extension (str): File extension (e.g., "parquet", "csv")
        
    Returns:
        str: Constructed filename
    """
    return f"{file_prefix}_{date_param}.{file_extension}"


def construct_s3_key(s3_key_pattern, **kwargs):
    """
    Construct S3 key using pattern and parameters
    
    Args:
        s3_key_pattern (str): Pattern with placeholders (e.g., "landing/{source}/year={year}/month={month}/{filename}")
        **kwargs: Values to substitute in pattern
        
    Returns:
        str: Constructed S3 key
        
    Example:
        construct_s3_key(
            "landing/{source}/year={year}/month={month}/{filename}",
            source="yellow-trips",
            year="2024",
            month="01",
            filename="yellow_tripdata_2024-01.parquet"
        )
        # Returns: "landing/yellow-trips/year=2024/month=01/yellow_tripdata_2024-01.parquet"
    """
    return s3_key_pattern.format(**kwargs)


def lambda_handler(event, context):
    """
    Generic HTTP-to-S3 ingestion handler
    
    This function downloads data from any HTTP endpoint and uploads to S3.
    ALL configuration is provided via the event parameter.
    
    Required event parameters:
        - base_url: Base URL of the data source
        - file_prefix: Prefix for filename construction
        - date_param: Date parameter for filename (e.g., "2024-01")
        - s3_key_pattern: S3 key pattern with placeholders
        
    Optional event parameters:
        - file_extension: File extension (default: "parquet")
        - bucket_name: S3 bucket (default: from environment variable)
        - check_exists: Whether to check file exists first (default: true)
        - timeout: Download timeout in seconds (default: 300)
        - metadata: Additional S3 object metadata (dict)
        
    Returns:
        dict: Response with status code and body
        
    Example event for NYC TLC:
        {
            "base_url": "https://d37ci6vzurychx.cloudfront.net/trip-data",
            "file_prefix": "yellow_tripdata",
            "date_param": "2024-01",
            "file_extension": "parquet",
            "s3_key_pattern": "landing/yellow-trips/year={year}/month={month}/{filename}",
            "bucket_name": "my-data-lake-bucket",
            "source": "yellow-trips",
            "year": "2024",
            "month": "01"
        }
        
    Example event for Stripe API:
        {
            "base_url": "https://api.stripe.com/v1/exports",
            "file_prefix": "transactions",
            "date_param": "2024-01-15",
            "file_extension": "json.gz",
            "s3_key_pattern": "landing/stripe/date={date}/{filename}",
            "bucket_name": "my-data-lake-bucket",
            "date": "2024-01-15"
        }
    """
    try:
        # ====================================================================
        # 1. VALIDATE REQUIRED PARAMETERS
        # ====================================================================
        required_params = ['base_url', 'file_prefix', 'date_param', 's3_key_pattern']
        missing_params = [p for p in required_params if p not in event]
        
        if missing_params:
            error_msg = f"Missing required parameters: {', '.join(missing_params)}"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': {'error': error_msg}
            }
        
        # ====================================================================
        # 2. EXTRACT PARAMETERS
        # ====================================================================
        base_url = event['base_url']
        file_prefix = event['file_prefix']
        date_param = event['date_param']
        s3_key_pattern = event['s3_key_pattern']
        
        # Optional parameters with defaults
        file_extension = event.get('file_extension', 'parquet')
        bucket_name = event.get('bucket_name', os.environ.get('BUCKET_NAME'))
        check_exists = event.get('check_exists', True)
        timeout = event.get('timeout', DEFAULT_TIMEOUT)
        custom_metadata = event.get('metadata', {})
        
        if not bucket_name:
            raise ValueError("bucket_name must be provided in event or BUCKET_NAME environment variable")
        
        # ====================================================================
        # 3. CONSTRUCT URL AND S3 KEY
        # ====================================================================
        filename = construct_filename(file_prefix, date_param, file_extension)
        url = f"{base_url.rstrip('/')}/{filename}"
        
        # Construct S3 key - pass all event params for flexibility
        s3_key_params = {
            'filename': filename,
            **{k: v for k, v in event.items() if k not in required_params}
        }
        s3_key = construct_s3_key(s3_key_pattern, **s3_key_params)
        
        logger.info(f"Configuration:")
        logger.info(f"  URL: {url}")
        logger.info(f"  S3: s3://{bucket_name}/{s3_key}")
        
        # ====================================================================
        # 4. CHECK FILE EXISTENCE (if enabled)
        # ====================================================================
        if check_exists:
            logger.info(f"Checking file existence...")
            if not check_file_exists(url, timeout=10):
                logger.warning(f"File does not exist: {url}")
                return {
                    'statusCode': 404,
                    'body': {
                        'message': 'File not found - data not yet available',
                        'url': url,
                        'skip': True,
                        'reason': 'Data not available for this period'
                    }
                }
        
        # ====================================================================
        # 5. DOWNLOAD FILE
        # ====================================================================
        logger.info(f"Downloading file (timeout={timeout}s)...")
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        
        file_size_mb = len(response.content) / (1024 * 1024)
        logger.info(f"Downloaded {file_size_mb:.2f} MB")
        
        # ====================================================================
        # 6. UPLOAD TO S3 WITH METADATA
        # ====================================================================
        metadata = {
            'source_url': url,
            'ingestion_timestamp': datetime.utcnow().isoformat(),
            'date_param': date_param,
            'file_size_bytes': str(len(response.content)),
            **custom_metadata  # Allow caller to add custom metadata
        }
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=response.content,
            Metadata=metadata
        )
        
        logger.info(f"Successfully uploaded to S3")
        
        # ====================================================================
        # 7. RETURN SUCCESS RESPONSE
        # ====================================================================
        return {
            'statusCode': 200,
            'body': {
                'message': 'Upload successful',
                'url': url,
                'filename': filename,
                's3_bucket': bucket_name,
                's3_key': s3_key,
                'size_bytes': len(response.content),
                'size_mb': round(file_size_mb, 2)
            }
        }
        
    except requests.exceptions.Timeout:
        logger.error(f"Request timeout after {timeout} seconds")
        return {
            'statusCode': 408,
            'body': {'error': f'Request timeout - download exceeded {timeout} seconds'}
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Download failed: {e}")
        return {
            'statusCode': 500,
            'body': {'error': f'Download failed: {str(e)}'}
        }
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        return {
            'statusCode': 400,
            'body': {'error': str(e)}
        }
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': {'error': f'Internal error: {str(e)}'}
        }
