"""
Example script: upload dealer CSV to S3 raw path (Extract step).

Prerequisites:
  - boto3, python-dotenv: pip install -r requirements.txt
  - AWS credentials: set in .env (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION)
    or use aws configure / env vars
  - S3 bucket and prefix (BUCKET, RAW_PREFIX in .env or env)

Usage:
  python extract/upload_csv_to_s3.py
"""

import os
import boto3
from dotenv import load_dotenv

# Load .env from repo root so BUCKET, RAW_PREFIX, and AWS_* are set
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# Configure these or set in .env / env vars BUCKET, RAW_PREFIX
BUCKET = os.environ.get("BUCKET", "your-bucket-name")
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/dealer-sales-data")
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
