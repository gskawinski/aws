import json
import requests
import boto3
from datetime import datetime


def lambda_handler(event, context):
    """
    AWS Lambda function to fetch the top 100 tokens by market cap from the CoinGecko API,
    save the data to an S3 bucket, and return a success message.

    Parameters:
    event (dict): Lambda invocation event
    context (LambdaContext): Lambda runtime information

    Returns:
    dict: Response object containing status code and message
    """
    try:
        # Fetch top 100 tokens from CoinGecko API
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 100,
            "page": 1,
        }
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an HTTPError on bad response
        tokens = response.json()

        # Get current timestamp for logging and file naming
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        file_timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")

        # Define S3 bucket and object key
        s3_bucket_name = "bucket-name"
        s3_key = f"coingecko_top100_{file_timestamp}.json"

        # Save data to S3 as a JSON file
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket=s3_bucket_name,
            Key=s3_key,
            Body=json.dumps(tokens),
            ContentType="application/json",
        )

        # Log success and return response
        print(
            f"[{timestamp}] Top 100 tokens data saved to S3 bucket '{s3_bucket_name}' with key '{s3_key}'."
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Top 100 tokens data saved to S3 successfully!",
                    "timestamp": timestamp,
                    "s3_key": s3_key,
                }
            ),
        }
    except Exception as e:
        # Log error and return failure response
        error_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{error_timestamp}] Error occurred: {str(e)}")

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "Failed to save top 100 tokens data to S3.",
                    "timestamp": error_timestamp,
                    "error": str(e),
                }
            ),
        }


# =======================
# Web Scrapping
# Lambda function that uses BeautifulSoup for web scraping to fetch the top 100 coins from CoinGecko, and then saves the data to an S3 bucket

import json
import requests
import boto3
from bs4 import BeautifulSoup
from datetime import datetime


def lambda_handler(event, context):
    """
    AWS Lambda function to fetch the top 100 coins by market cap from CoinGecko using web scraping,
    save the data to an S3 bucket, and return a success message.

    Parameters:
    event (dict): Lambda invocation event
    context (LambdaContext): Lambda runtime information

    Returns:
    dict: Response object containing status code and message
    """
    try:
        # Fetch the CoinGecko webpage
        url = "https://www.coingecko.com/en"
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError on bad response

        # Parse the webpage content
        soup = BeautifulSoup(response.text, "html.parser")

        # Find the table containing the top 100 coins
        table = soup.find("table", {"class": "table-scrollable"})
        rows = table.find_all("tr")[1:101]  # Skipping the header row

        # Extract data for each coin
        data = []
        for row in rows:
            cols = row.find_all("td")
            coin = {
                "rank": cols[0].text.strip(),
                "name": cols[1].text.strip(),
                "symbol": cols[2].text.strip(),
                "market_cap": cols[3].text.strip(),
                "price": cols[4].text.strip(),
                "volume_24h": cols[5].text.strip(),
                "circulating_supply": cols[6].text.strip(),
            }
            data.append(coin)

        # Get current timestamp for logging and file naming
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        file_timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")

        # Define S3 bucket and object key
        s3_bucket_name = "bucket-name"
        s3_key = f"coingecko_top100_{file_timestamp}.json"

        # Save data to S3 as a JSON file
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket=s3_bucket_name,
            Key=s3_key,
            Body=json.dumps(data),
            ContentType="application/json",
        )

        # Log success and return response
        print(
            f"[{timestamp}] Top 100 coins data saved to S3 bucket '{s3_bucket_name}' with key '{s3_key}'."
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Top 100 coins data saved to S3 successfully!",
                    "timestamp": timestamp,
                    "s3_key": s3_key,
                    "data": data,
                }
            ),
        }
    except Exception as e:
        # Log error and return failure response
        error_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{error_timestamp}] Error occurred: {str(e)}")

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "Failed to save top 100 coins data to S3.",
                    "timestamp": error_timestamp,
                    "error": str(e),
                }
            ),
        }
