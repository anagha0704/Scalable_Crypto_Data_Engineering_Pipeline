import requests
import json
import os
from dotenv import load_dotenv
import boto3
from datetime import datetime
import logging
import time

load_dotenv() 

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")

SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")  # Store this in Lambda env variables


# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def send_alert(subject, message):
    client = boto3.client("sns")
    client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )

def lambda_handler(event, context):
    try:
        logger.info("Starting lambda_handler execution.")

        run_type = event.get("run_type")
        if run_type == "intra_day":
            logger.warning("Running in intra_day mode.")
        else:
            logger.warning(f"Running in {run_type} mode.")

        # Fetch coins data from CoinGecko API
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 25,
            "page": 1,
            "sparkline": False
            }
        headers = {
            "accept": "application/json",
            "x-cg-demo-api-key": COINGECKO_API_KEY
        }

        try:
            logger.info("Requesting data from CoinGecko API with exponential backoff.")
            max_retries = 5
            backoff = 1  # seconds

            for attempt in range(1, max_retries + 1):
                try:
                    logger.info(f"Attempt {attempt} to fetch data from CoinGecko.")
                    response = requests.get(url, params=params, headers=headers)
                    response.raise_for_status()
                    break  # Success, exit loop
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Attempt {attempt} failed: {str(e)}")
                    if attempt == max_retries:
                        raise
                    time.sleep(backoff)
                    backoff *= 2  # Exponential backoff
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from CoinGecko: {str(e)}")
            send_alert(
                subject="Lambda Error: Crypto Fetch Failed",
                message=f"Error fetching data from CoinGecko:\n{str(e)}"
            )
            raise

        try:
            coins_data = response.json()
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON response: {str(e)}")
            send_alert(
                subject="Lambda Error: Crypto Fetch Failed",
                message=f"Error decoding JSON response:\n{str(e)}"
            )
            raise
        
        for i,coin in enumerate(coins_data, start=1):


            url = f"https://api.coingecko.com/api/v3/coins/{coin['id']}/market_chart"

            print(f"Fetching Coin {i}: {coin['name']} ID({coin['id']}) ({coin['symbol'].upper()})")

            params = {
                "vs_currency": "usd",
                "days": 1 if run_type == "intra_day" else 365,
                "precision": 2
                }
                

            response = requests.get(url, headers=headers, params=params)

            coin_data = response.json()

            coin_data_final = {
                "coin_id": coin["id"],
                "coin_name": coin["name"],
                "coin_symbol": coin["symbol"],
                "coin_image": coin["image"],
                "market_cap_rank": coin["market_cap_rank"],
                "data": coin_data
                }

            # Store JSON data into S3
            try:
                s3 = boto3.client("s3")
                bucket_name = "crypto-raw-data-0704"
                s3_key = f"intra_day_data/{coin['id']}.json" if run_type == "intra_day" else f"historical_data/{coin['id']}.json"
                logger.info(f"Uploading data to s3://{bucket_name}/{s3_key}")
                s3.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=json.dumps(coin_data_final),
                    ContentType="application/json"
                    )
            except Exception as e:
                logger.error(f"Error uploading to S3: {str(e)}")
                send_alert(
                    subject="Lambda Error: Crypto Fetch Failed",
                    message=f"Error uploading to S3:\n{str(e)}"
                        )
                raise

        logger.info(f"Successfully stored coins data to s3://{bucket_name}/{s3_key}")
        send_alert(
            subject=f"Lambda Alerts: Crypto Fetch Success - {run_type.replace('_', ' ').title()}",
            message=f"Successfully stored coins data to s3://{bucket_name}/{s3_key.split('/')[0]}"
        )

        return {
            "statusCode": 200,
            "body": f"Successfully stored coins data to s3://{bucket_name}/{s3_key.split('/')[0]}",
            "success": True
        }
    except Exception as e:
        logger.exception("Unhandled exception in lambda_handler.")
        # Signal Lambda failure by raising the exception
        raise


# lambda_handler({"run_type": "normal"}, None)  # For local testing, remove in production