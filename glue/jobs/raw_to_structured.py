"""
Glue Job: Raw → Structured Zone
Data Transformations and Feature Engineering

Features:
- Type casting and standardization
- Time-series feature engineering for forecasting
- Calculated business metrics
- Outlier removal (configurable thresholds)
- Optimized partitioning for Athena
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *

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
raw_path = f"s3://{BUCKET}/raw/{source}/year={year}/month={month_num}/"
structured_path = f"s3://{BUCKET}/structured/{source}/"

print(f"=" * 80)
print(f"Glue Job: Raw → Structured")
print(f"Processing: {raw_path}")
print(f"=" * 80)

try:
    # ========================================================================
    # 1. READ FROM RAW ZONE
    # ========================================================================
    df = spark.read.parquet(raw_path)
    initial_count = df.count()
    print(f"Records to process: {initial_count:,}")
    
    # ========================================================================
    # 2. DATA TYPE CASTING & STANDARDIZATION
    # ========================================================================
    print("\n--- Type Casting ---")
    
    df = df.withColumn("VendorID", F.col("VendorID").cast(IntegerType()))
    df = df.withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
    df = df.withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
    df = df.withColumn("RatecodeID", F.col("RatecodeID").cast(IntegerType()))
    df = df.withColumn("PULocationID", F.col("PULocationID").cast(IntegerType()))
    df = df.withColumn("DOLocationID", F.col("DOLocationID").cast(IntegerType()))
    df = df.withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
    
    # Financial columns
    df = df.withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
    df = df.withColumn("extra", F.col("extra").cast(DoubleType()))
    df = df.withColumn("mta_tax", F.col("mta_tax").cast(DoubleType()))
    df = df.withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))
    df = df.withColumn("tolls_amount", F.col("tolls_amount").cast(DoubleType()))
    df = df.withColumn("improvement_surcharge", F.col("improvement_surcharge").cast(DoubleType()))
    df = df.withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
    
    # Handle optional columns
    if "congestion_surcharge" in df.columns:
        df = df.withColumn("congestion_surcharge", F.col("congestion_surcharge").cast(DoubleType()))
    if "airport_fee" in df.columns:
        df = df.withColumn("airport_fee", F.col("airport_fee").cast(DoubleType()))
    
    # Datetime columns
    df = df.withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType()))
    df = df.withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))
    
    print("✓ Type casting complete")
    
    # ========================================================================
    # 3. FEATURE ENGINEERING - Time-based Features
    # ========================================================================
    print("\n--- Feature Engineering: Time-based ---")
    
    # Extract date components (for forecasting/seasonality analysis)
    df = df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    df = df.withColumn("pickup_year", F.year("tpep_pickup_datetime"))
    df = df.withColumn("pickup_month", F.month("tpep_pickup_datetime"))
    df = df.withColumn("pickup_day", F.dayofmonth("tpep_pickup_datetime"))
    df = df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    df = df.withColumn("pickup_dayofweek", F.dayofweek("tpep_pickup_datetime"))
    
    # Week of year (for weekly seasonality)
    df = df.withColumn("pickup_week", F.weekofyear("tpep_pickup_datetime"))
    
    # Is weekend flag
    df = df.withColumn("is_weekend", 
        F.when(F.col("pickup_dayofweek").isin([1, 7]), 1).otherwise(0)
    )
    
    print("✓ Time-based features created")
    
    # ========================================================================
    # 4. FEATURE ENGINEERING - Calculated Metrics
    # ========================================================================
    print("\n--- Feature Engineering: Calculated Metrics ---")
    
    # Trip duration in minutes
    df = df.withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime") - 
         F.unix_timestamp("tpep_pickup_datetime")) / 60
    )
    
    # Average speed (mph)
    df = df.withColumn(
        "avg_speed_mph",
        F.when(
            (F.col("trip_duration_minutes") > 0) & (F.col("trip_distance") > 0),
            (F.col("trip_distance") / F.col("trip_duration_minutes")) * 60
        ).otherwise(None)
    )
    
    # Fare per mile
    df = df.withColumn(
        "fare_per_mile",
        F.when(
            F.col("trip_distance") > 0,
            F.col("fare_amount") / F.col("trip_distance")
        ).otherwise(None)
    )
    
    # Tip percentage (for credit card payments)
    df = df.withColumn(
        "tip_percentage",
        F.when(
            (F.col("payment_type") == 1) & (F.col("fare_amount") > 0),
            (F.col("tip_amount") / F.col("fare_amount")) * 100
        ).otherwise(None)
    )
    
    # Total cost per passenger
    df = df.withColumn(
        "cost_per_passenger",
        F.when(
            F.col("passenger_count") > 0,
            F.col("total_amount") / F.col("passenger_count")
        ).otherwise(None)
    )
    
    print("✓ Calculated metrics created")
    
    # ========================================================================
    # 5. ADD PARTITION COLUMN
    # ========================================================================
    # Partition by date for efficient Athena queries
    df = df.withColumn("ds", F.date_format("pickup_date", "yyyy-MM-dd"))
    
    # ========================================================================
    # 6. OUTLIER REMOVAL (Optional but recommended for analytics)
    # ========================================================================
    print("\n--- Outlier Removal ---")
    
    df_clean = df.filter(
        # Reasonable trip durations
        (F.col("trip_duration_minutes") > 0) &
        (F.col("trip_duration_minutes") < 1440) &  # Less than 24 hours
        
        # Reasonable distances
        (F.col("trip_distance") > 0) &
        (F.col("trip_distance") < 100) &  # Less than 100 miles (99th percentile)
        
        # Reasonable fares
        (F.col("fare_amount") > 0) &
        (F.col("fare_amount") < 500) &  # Less than $500 (99th percentile)
        
        # Reasonable speed (not physically impossible)
        ((F.col("avg_speed_mph").isNull()) | 
         ((F.col("avg_speed_mph") > 0) & (F.col("avg_speed_mph") < 100)))  # Max 100 mph
    )
    
    clean_count = df_clean.count()
    outliers_removed = initial_count - clean_count
    outlier_percentage = (outliers_removed / initial_count * 100) if initial_count > 0 else 0
    
    print(f"  Records after outlier removal: {clean_count:,}")
    print(f"  Outliers removed: {outliers_removed:,} ({outlier_percentage:.2f}%)")
    
    # ========================================================================
    # 7. WRITE TO STRUCTURED ZONE (Partitioned)
    # ========================================================================
    print("\n--- Writing to Structured Zone ---")
    
    # Coalesce to reduce number of small files (optimal: 128-512 MB per file)
    # Adjust based on data size
    num_partitions = max(1, clean_count // 100000)  # ~100K records per partition
    
    print(f"  Writing {clean_count:,} records")
    print(f"  Partitioning by: ds (date)")
    print(f"  Coalescing to {num_partitions} partitions")
    
    df_clean.coalesce(num_partitions).write \
        .mode("append") \
        .partitionBy("ds") \
        .parquet(structured_path)
    
    print(f"✓ Successfully wrote to: {structured_path}")
    
    # ========================================================================
    # 8. SUMMARY STATISTICS
    # ========================================================================
    print("\n--- Data Statistics ---")
    
    stats = df_clean.select(
        F.count("*").alias("total_trips"),
        F.sum("fare_amount").alias("total_fare"),
        F.avg("trip_distance").alias("avg_distance"),
        F.avg("trip_duration_minutes").alias("avg_duration"),
        F.avg("avg_speed_mph").alias("avg_speed"),
        F.avg("fare_per_mile").alias("avg_fare_per_mile")
    ).collect()[0]
    
    print(f"  Total trips:         {stats['total_trips']:,}")
    print(f"  Total fare:          ${stats['total_fare']:,.2f}")
    print(f"  Avg distance:        {stats['avg_distance']:.2f} miles")
    print(f"  Avg duration:        {stats['avg_duration']:.2f} minutes")
    print(f"  Avg speed:           {stats['avg_speed']:.2f} mph")
    print(f"  Avg fare per mile:   ${stats['avg_fare_per_mile']:.2f}")
    
    # ========================================================================
    # 9. JOB SUMMARY
    # ========================================================================
    print("\n" + "=" * 80)
    print("JOB SUMMARY")
    print("=" * 80)
    print(f"Input:              {initial_count:,} records")
    print(f"After outliers:     {clean_count:,} records")
    print(f"Outliers removed:   {outliers_removed:,} ({outlier_percentage:.2f}%)")
    print(f"Features created:   11 (time-based + calculated)")
    print(f"Partition column:   ds (YYYY-MM-DD)")
    print(f"Status:             ✓ SUCCESS")
    print("=" * 80)
    
    job.commit()
    
except Exception as e:
    print(f"\n❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    job.commit()
    raise e
