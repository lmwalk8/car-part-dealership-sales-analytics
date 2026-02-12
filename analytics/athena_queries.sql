-- =============================================================================
-- Athena: create table and example queries over processed dealer Parquet data
-- =============================================================================
-- Replace 's3://your-bucket/processed/dealer-licenses/' with your actual path.
-- Run "CREATE EXTERNAL TABLE" once; then use the SELECTs for analysis.
-- =============================================================================

-- Create external table over Glue job output (run once per database/path)
CREATE EXTERNAL TABLE IF NOT EXISTS dealer_licenses (
  LICYR int,
  LICTYPE string,
  LICNO string,
  BUSNAME string,
  OWNERSHIP string,
  LOTADDR string,
  LOTCITY string,
  LOTSTATE string,
  PHONE string,
  EST_AVG_YEARLY_SALES int
)
STORED AS PARQUET
LOCATION 's3://your-bucket/processed/dealer-licenses/';

-- -----------------------------------------------------------------------------
-- Example queries
-- -----------------------------------------------------------------------------

-- License counts and average estimated sales by license type
SELECT
  LICTYPE,
  COUNT(*) AS license_count,
  ROUND(AVG(EST_AVG_YEARLY_SALES), 0) AS avg_est_sales
FROM dealer_licenses
GROUP BY LICTYPE
ORDER BY license_count DESC;

-- Top cities by dealer count and total estimated sales
SELECT
  LOTCITY,
  COUNT(*) AS dealers,
  SUM(EST_AVG_YEARLY_SALES) AS total_est_sales
FROM dealer_licenses
WHERE LOTSTATE = 'OK'
GROUP BY LOTCITY
ORDER BY total_est_sales DESC
LIMIT 20;

-- Ownership type distribution
SELECT
  LICTYPE,
  COUNT(*) AS count_
FROM dealer_licenses
GROUP BY LICTYPE
ORDER BY count_ DESC;

-- All rows for selected dealership type
SELECT *
FROM dealer_licenses
WHERE LICTYPE = 'AD'
ORDER BY BUSNAME;

-- Sample rows for QuickSight / validation
SELECT *
FROM dealer_licenses
LIMIT 100;
