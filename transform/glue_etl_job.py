"""
Example AWS Glue PySpark job: Transform raw dealer CSV and load as Parquet.

- Reads CSV from S3 raw path (header, quoted fields).
- Drops duplicates on (LICNO, LICTYPE), drops rows with null BUSNAME or LOTCITY.
- Adds EST_AVG_YEARLY_SALES by LICTYPE (example benchmarks; replace with your logic).
- Writes Parquet to S3 processed path.

In Glue console:
  1. Create a Glue Job, type "Spark script".
  2. Paste this script (or upload from S3).
  3. Set job parameters or hardcode raw_path / processed_path below.
  4. Use a role with read/write to the S3 paths.

Running locally:
  - awsglue is not on PyPI; it ships only in the Glue runtime. Two options:
    1) Docker: use AWS Glue libs image, e.g.:
       docker run -it --rm -v $(pwd):/home/hadoop/workspace \\
         -e AWS_PROFILE=your_profile public.ecr.aws/glue/aws-glue-libs:5 \\
         spark-submit /home/hadoop/workspace/transform/glue_etl_job.py
    2) Use run_etl_local.py instead (plain PySpark, no awsglue); same logic.
"""

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, coalesce, when
from pyspark.sql.types import IntegerType

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Set in Glue job parameters or replace with your bucket/prefixes
raw_path = "s3://your-bucket/raw/dealer-licenses/"
processed_path = "s3://your-bucket/processed/dealer-licenses/"

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

# Load: write Parquet to processed path
df.write.mode("overwrite").parquet(processed_path)

job.commit()
