"""
Example script: upload dealer CSV to S3 raw path (Extract step).

Prerequisites:
  - boto3: pip install boto3
  - AWS credentials configured (env vars, ~/.aws/credentials, or IAM role)
  - S3 bucket and prefix created (or script will use existing bucket)

Usage:
  python scripts/upload_csv_to_s3.py
  # Or with env override:
  BUCKET=my-bucket RAW_PREFIX=raw/dealer-licenses python scripts/upload_csv_to_s3.py
"""

import os
import boto3

# Configure these or set env vars BUCKET, RAW_PREFIX
BUCKET = os.environ.get("BUCKET", "your-bucket-name")
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/dealer-licenses")
# Path relative to repo root
LOCAL_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "data-motor-vehicle-dealer-sales.csv")
OBJECT_KEY = f"{RAW_PREFIX.rstrip('/')}/data-motor-vehicle-dealer-sales.csv"


def main():
    if not os.path.isfile(LOCAL_CSV):
        raise FileNotFoundError(f"CSV not found: {LOCAL_CSV}")
    s3 = boto3.client("s3")
    s3.upload_file(LOCAL_CSV, BUCKET, OBJECT_KEY)
    print(f"Uploaded to s3://{BUCKET}/{OBJECT_KEY}")


if __name__ == "__main__":
    main()
