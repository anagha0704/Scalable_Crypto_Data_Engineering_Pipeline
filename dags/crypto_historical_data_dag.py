from dotenv import load_dotenv
import os
from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.notifications.sns import send_sns_notification

load_dotenv()

# Fetching variables from .env
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "ENV_VAR_NOT_FOUND")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
GLUE_ROLE = os.getenv("GLUE_ROLE")
SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_connection")
AWS_CONN = os.getenv("AWS_CONN_ID", "aws_default")

# Building the S3 Path dynamically
S3_PATH = f"s3://{os.getenv('S3_SCRIPT_BUCKET')}/{os.getenv('HISTORICAL_SCRIPT_PATH')}"

default_args = {
    'owner': 'airflow',
    'on_failure_callback': send_sns_notification(
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        target_arn=SNS_TOPIC_ARN,
        message="❌ Historical Pipeline Failed! Task: {{ ti.task_id }}",
        subject="Airflow Alert: Historical Failure"
    )
}

with DAG(
    dag_id='crypto_data_pipeline_historical',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None, 
    catchup=False,
    tags=['crypto', 'historical', 'sns']
) as dag:

    # 1. Trigger Lambda (Historical Mode)
    run_lambda = LambdaInvokeFunctionOperator(
        task_id="run_lambda_historical",
        function_name="crypto_ingestion",
        payload='{"run_type": "normal"}', # Matches your script's logic
        aws_conn_id="aws_default"
    )

    # 2. Run Glue Transformation
    run_glue = GlueJobOperator(
        task_id="run_glue_historical",
        job_name="historical_data_transformation",
        script_location=S3_PATH,
        iam_role_name=GLUE_ROLE,
        region_name=AWS_REGION,
        aws_conn_id=AWS_CONN,
        create_job_kwargs={"Connections": {"Connections": ["Snowflake"]}}
    )

    # 3. Clean Snowflake Table
    truncate_table = SQLExecuteQueryOperator(
        task_id="truncate_snowflake_historical",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="TRUNCATE TABLE CRYPTO_DB.TRANSFORMED.CRYPTO_HISTORICAL_DATA;"
    )

    # 4. Refresh Snowpipe
    refresh_pipe = SQLExecuteQueryOperator(
        task_id="refresh_historical_pipe",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="ALTER PIPE CRYPTO_DB.TRANSFORMED.MYPIPE_HISTORICAL REFRESH;"
    )

    run_lambda >> run_glue >> truncate_table >> refresh_pipe