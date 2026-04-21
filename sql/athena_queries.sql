-- ============================================================================
-- Athena SQL Queries for NYC TLC Data Lake
-- ============================================================================

-- 1. REPAIR TABLE - Discover new partitions after Glue job
-- ============================================================================
MSCK REPAIR TABLE yellow_trips;

-- 2. BASIC VALIDATION QUERIES
-- ============================================================================

-- Total record count
SELECT COUNT(*) AS total_trips FROM yellow_trips;

-- Records by partition (date)
SELECT ds, COUNT(*) AS trip_count
FROM yellow_trips
GROUP BY ds
ORDER BY ds DESC;

-- Records by year and month
SELECT pickup_year, pickup_month,
       COUNT(*) AS trip_count,
       ROUND(SUM(fare_amount), 2) AS total_fare,
       ROUND(AVG(trip_distance), 2) AS avg_distance
FROM yellow_trips
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year DESC, pickup_month DESC;

-- 3. DATA QUALITY CHECKS
-- ============================================================================

-- Check for nulls
SELECT COUNT(*) AS total,
       SUM(CASE WHEN fare_amount IS NULL THEN 1 ELSE 0 END) AS null_fare
FROM yellow_trips;

-- 4. BUSINESS ANALYTICS
-- ============================================================================

-- Trips by hour (peak hours)
SELECT pickup_hour, COUNT(*) AS trips, AVG(fare_amount) AS avg_fare
FROM yellow_trips
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- Payment type distribution
SELECT payment_type, COUNT(*) AS trips
FROM yellow_trips
GROUP BY payment_type;

--5. QUARANTINE REVIEW
-- ============================================================================

-- Count quarantined records by reason
SELECT quarantine_reason, COUNT(*) AS count
FROM quarantine_yellow_trips
GROUP BY quarantine_reason
ORDER BY count DESC;
