"""
Same ETL logic as glue_etl_job.py but using only PySpark (no awsglue).

Run from repo root:

  python transform/run_etl_local.py

Optional: pass input CSV and output path (defaults: data/... and data/processed):

  python transform/run_etl_local.py data/data-motor-vehicle-dealer-sales.csv data/processed

Requires: pip install pyspark and Java 8 or 11 on PATH.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, when
from pyspark.sql.types import IntegerType

# Default paths: local CSV and local output (override with args or env)
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_RAW = os.path.join(REPO_ROOT, "data", "data-motor-vehicle-dealer-sales.csv")
DEFAULT_PROCESSED = os.path.join(REPO_ROOT, "data", "processed")

raw_path = os.environ.get("RAW_PATH", DEFAULT_RAW)
processed_path = os.environ.get("PROCESSED_PATH", DEFAULT_PROCESSED)
if len(sys.argv) >= 3:
    raw_path = sys.argv[1]
    processed_path = sys.argv[2]
elif len(sys.argv) == 1:
    pass  # use defaults
else:
    print("Usage: python transform/run_etl_local.py [raw_csv_path] [output_path]", file=sys.stderr)
    sys.exit(1)

spark = (
    SparkSession.builder.appName("dealer-etl-local")
    .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
    .getOrCreate()
)

# Read CSV (header, handle quoted commas in BUSNAME)
df = (
    spark.read.option("header", "true")
    .option("quote", '"')
    .csv(raw_path)
)

# Clean: drop duplicates by license identifier, drop null key fields
df = df.dropDuplicates(["LICNO", "LICTYPE"]).dropna(subset=["BUSNAME", "LOTCITY"])

# Example: estimated average yearly sales by license type (benchmarks only)
benchmarks = {"UD": 850000, "AD": 420000, "MH": 310000}
expr = coalesce(*[when(col("LICTYPE") == k, v) for k, v in benchmarks.items()])
df = df.withColumn("EST_AVG_YEARLY_SALES", expr.cast(IntegerType()))

# Load: write Parquet
df.write.mode("overwrite").parquet(processed_path)
print(f"Wrote Parquet to {processed_path}")
spark.stop()
