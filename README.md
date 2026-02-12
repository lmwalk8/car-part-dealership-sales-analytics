# Oklahoma Dealer License Sales Analytics

ETL pipeline, analysis and interactive visualization of Oklahoma Used Motor Vehicle and Parts Commission dealer license data done in AWS.

## Project Overview:

- Extract dealer data from public CSV file and place in S3 bucket.
- Transform raw data in AWS Glue using PySpark. Clean up data, remove duplicates/nulls, and add a column of estimated yearly sales for each row to use in future analysis.
- Load back into S3 bucket in Parquet format.
- Processed data is queried and analyzed in Amazon Athena with interactice displays in Amazon QuickSight.

## Technology Stack (Prerequisites to Run Project):

### Required

- Existing AWS account:
    - Services Used:
        - `S3`: To store raw and transformed data.
        - `Glue`: To tranform data into a more useful dataset.
        - `Athena`: To analyze transformed data through various queries.
        - `QuickSight`: To display data analysis in an interactive dashboard.

### Optional (if running/developing/testing scripts locally)

- Python 3.7+
    - Version required for PySpark compatibility
    - Libraries Used:
        - `pyspark`: For data processing.
        - `boto3`: For AWS connection.
- Java JDK 8 or 11
    - Version required for PySpark

## Data Source and Estimates

- **Source:** [data.ok.gov — Used Motor Vehicle and Parts Commission Dealer License Search](https://data.ok.gov/dataset/used-motor-vehicle-and-parts-commission-dealer-license-search).
- **`EST_AVG_YEARLY_SALES`** is an randomly generated, estimated average by license type (industry benchmarks), not reported figures. Used for relative comparison only as a part of this project.

## Project Setup:

### Required:

1. Install/create project dependencies if applicable (AWS account, Python, Java)

2. Clone this repository:
```
git clone https://github.com/lmwalk8/car-part-dealership-sales-analytics.git
cd car-part-dealership-sales-analytics
```

### Optional (if running ETL scripts locally):

3. Create and activate a Python virutal environment:
```
python3 -m venv dealership_sales_project_env
source dealership_sales_project_env/bin/activate (Linux/macOS) OR dealership_sales_project_env\Scripts\activate.bat (Windows)
```

4. Install all required dependencies:
```
pip install -r requirements.txt
```

5. Run any scripts:

- Upload raw CSV to AWS S3 bucket:
```
python upload_csv_to_s3.py
```

- ETL job (local option, not Glue script):
```
python run_etl_local.py
```

*Feel free to change any of these scripts as necessary to fit any personal AWS or local needs.*

## AWS Implementation Overview

End-to-end flow: **Extract (CSV → S3) → Transform (Glue PySpark) → Load (Parquet → S3) → Analyze (Athena) → Visualize (QuickSight)**.

| Step | Where it runs | What happens |
|------|----------------|--------------|
| **1. Extract** | Local or Lambda | Get dealer CSV (e.g. from data.ok.gov or repo), upload to S3 raw prefix. |
| **2. Transform** | AWS Glue | Glue job runs PySpark: read CSV from S3, clean/dedupe, add `EST_AVG_YEARLY_SALES`, write Parquet to S3 processed prefix. |
| **3. Load** | (same job) | Parquet output is the “loaded” dataset; no separate load step. |
| **4. Analyze** | Athena | Create table over the Parquet path, run SQL for aggregations and filters. |
| **5. Visualize** | QuickSight | Connect to Athena (or S3 dataset), build dashboards from those queries. |

Example artifacts in this repo:

- **`scripts/upload_csv_to_s3.py`** — Example: upload local CSV to an S3 raw path (Step 1).
- **`transform/glue_etl_job.py`** — Example Glue PySpark script: clean, dedupe, add estimated sales, write Parquet (Steps 2–3).
- **`analytics/athena_queries.sql`** — Example: create Athena table and sample queries (Step 4).

QuickSight: In QuickSight, create a dataset from the Athena table (or from the S3 Parquet location), then build analyses and dashboards from the same metrics you use in Athena (Step 5).

### 1. Extract: Upload CSV to S3

You can upload the dealer CSV manually, via AWS CLI, or with this Python script: `scripts/upload_csv_to_s3.py`

After the script, the raw data lives at `s3://your-bucket-name/raw/dealer-licenses/`.

### 2–3. Transform & Load: Glue PySpark job

In AWS Glue you create a **Job** that runs a PySpark script. The script reads the CSV from the raw S3 path, cleans/dedupes, adds random `EST_AVG_YEARLY_SALES` value by license type, and writes Parquet to a processed path. Example structure in `transform/glue_etl_job.py`:

In the Glue console you point the job at this script and upload the processed data into same S3 bucket we previously created.

*Note*
The job’s IAM role needs read/write to those S3 paths.

### 4. Analyze: Athena table and SQL

Create a table in Athena that points at the Parquet output, then run SQL queries (examples: `analytics/athena_queries.sql`)

### 5. Visualize: QuickSight

In QuickSight, create a **Dataset** from the Athena data source and choose the `dealer_licenses` table (or a custom SQL query). Then create analyses and dashboards using the same dimensions and measures (e.g. `LICTYPE`, `LOTCITY`, `EST_AVG_YEARLY_SALES`, counts).
