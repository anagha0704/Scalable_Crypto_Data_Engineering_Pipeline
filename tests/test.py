from lambda_function import ingestion

def test_coingecko_api_key():
    assert ingestion.COINGECKO_API_KEY is not None, "COINGECKO_API_KEY should be set in .env file"


def test_ping_coingecko():
    url = "https://api.coingecko.com/api/v3/ping"

    headers = {
    "accept": "application/json",
    "x-cg-demo-api-key": ingestion.COINGECKO_API_KEY
        }

    response = ingestion.requests.get(url, headers=headers)
    response_json = response.json()
    assert response.status_code == 200, response_json["status"]["error_message"]


def test_s3_buckets():
    s3 = ingestion.boto3.client("s3")
    bucket_names = ["crypto-raw-data-0704", "crypto-transformed-data-0704"]
    for bucket_name in bucket_names:
        try:
            s3.head_bucket(Bucket=bucket_name)
        except Exception as e:
            assert False, f"Bucket {bucket_name} does not exist or is not accessible: {e}"


def test_glue_job_exists_and_is_runnable():
    glue = ingestion.boto3.client("glue")
    jobs_name = ["historical_data_transformation.py", "intra_day_transformation.py"]
    for job_name in jobs_name:
        try:
            response = glue.get_job(JobName=job_name)
            assert "Job" in response, f"Glue job {job_name} not found"
        except Exception as e:
            assert False, f"Glue job check failed: {e}"


def test_sns_alert_real():
    subject = "Test Alert - SNS Integration (CI/CD Github Actions)"
    message = "This is a real SNS alert triggered from test_send_alert_real"

    try:
        ingestion.send_alert(subject, message)
    except Exception as e:
        assert False, f"Failed to send SNS alert: {e}"