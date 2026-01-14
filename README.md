# Scalable Crypto Data Engineering Pipeline
## Project Overview

This project implements an end-to-end data engineering pipeline designed to ingest, process, and analyze real-time cryptocurrency market data.
The goal is to provide a robust infrastructure for tracking price fluctuations, trading volumes, and market sentiment across various crypto assets.

### Data Processing Streams
The pipeline is architected to handle two distinct data frequencies, ensuring both deep historical context and real-time market awareness.
1. **Historical Data Pipeline:** This is a manual, one-time (or occasional) process designed to build your baseline dataset
2. **Intra-day Data Pipeline:** It is automated, runs every 6 hours and only fetches the latest data.

### Solution Stack

| Component | Tool / Service |
| :--- | :--- |
| Data Ingestion | AWS Lambda |
| Data Transformation | AWS Glue (PySpark) |
| Storage | AWS S3 |
| Data Warehouse | Snowflake |
| Orchestration | Apache Airflow |
| CI/CD / Devops | Github Actions, Docker |
| Region Alignment | us-east-2 |
| Alerts/Observability | AWS SNS |

## Project Structure

```text
Scalable_Crypto_Data_Engineering_Pipeline/
└── .github/
  └── workflows/
    └── deploy_lambda_glue.yml
└── dags/
  └── crypto_historical_data_dag.py
  └── crypto_intra_day_dag.py
└── glue_jobs/
  └── historical_data_transformation.py
  └── intra_day_transformation.py
└── lambda_function/
  └── ingestion.py
└── snowflake_scripts/
  └── 1_creation_tables.sql
  └── 2_creating_pipe_for_historical.sql
  └── 3_creating_pipe_for_intra_day.sql
  └── 4_inspection.sq;
└── tests/ 
  └── test.py
└── .gitignore
└── README.md
└── docker-compose.yaml
└── requirements.txt
```

## Pipeline Workflow

1. **Extract:** The pipeline triggers an API call to fetch the latest prices for cryptocurrencies.

2. **Transform:** The JSON response is flattened, timestamps are normalized, and missing values are handled.

3. **Load:** The structured data is appended to the historical price table in the database.

## S3 Structure

```text
s3://crypto-raw-data-0704/
├── historical_data/
└── intra_day/

s3://crypto-transformed-data-0704/
├── historical_data/
└── intra_day/
```

## Testing

* Unit tests for:
    * API reachability (CoinGecko)
    * S3 buckets existence
    * Glue script validation
    * SNS alerts

## Installation & Setup

Follow these high-level steps to replicate the environment and deploy the pipeline.

### 1. Prerequisites

* AWS Account (Can be free tier also)
* Snowflake Account
* Python ≥ 3.9
* Docker & Docker Compose
* AWS CLI configured (with access keys and default region)
* GitHub repository connected
* CoinGecko API Key (Get yours now: https://www.coingecko.com/en/api)

### 2. AWS Infrastructure Setup

1. CLI Configuration: Configure the AWS CLI with appropriate regional and access credentials.
2. Storage (S3): Create raw and transformed data buckets for historical and intraday data.
3. Identity & Access Management (IAM): Provision roles with specific permissions for Lambda and AWS Glue.
4. Compute: Deploy the Lambda data-fetcher and upload PySpark scripts to the Glue environment.

### 3. Snowflake Data Warehouse

* Account Provisioning: Create the database, schema, and virtual warehouse.
* Access Control: Configure users and roles with appropriate security privileges.
* Data Ingestion: Configure Snowpipe to automate data loading from the AWS S3 transformed bucket.

### 4. Airflow Orchestration

* Environment: Initialize Airflow using Docker Compose for containerized management.
* External Connections: Configure the following integration providers in the Airflow UI:
    * AWS: For managing Glue and Lambda triggers.
    * Snowflake: For data warehouse operations and monitoring.

### 5. CI/CD Pipeline

* GitHub Actions: Ensure the workflow triggers are active to automate deployments whenever changes are pushed to:

    * lambda_function/
    * glue_jobs/
    * dags/

##  Notes
* Snowpipe cannot overwrite; truncation must be handled manually or via DAG before ingestion
