import json
import boto3
import urllib.request
import os
from datetime import datetime, timedelta

# AWS clients
s3 = boto3.client("s3")

# Environment variables
BUCKET = os.environ.get("BUCKET_NAME")
ADSCRIBE_API = os.environ.get("ADSCRIBE_API_URL")


def lambda_handler(event, context):

    try:

        # ------------------------------------------------
        # STEP 1: Calculate automatic date range
        # ------------------------------------------------
        today = datetime.utcnow().date()
        yesterday = today - timedelta(days=1)

        start_date = yesterday.strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")

        print(f"Fetching data from {start_date} to {end_date}")

        # ------------------------------------------------
        # STEP 2: Call Adscribe API
        # ------------------------------------------------
        req = urllib.request.Request(
            ADSCRIBE_API,
            data=json.dumps({
                "start_date": start_date,
                "end_date": end_date
            }).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST"
        )

        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read())

        download_url = data["download_url"]

        print(f"Download URL received: {download_url}")

        # ------------------------------------------------
        # STEP 3: Prepare Bronze S3 path
        # ------------------------------------------------
        ingestion_date = today.strftime("%Y-%m-%d")

        s3_key = (
            f"bronze/source=api/provider=adscribe/"
            f"date={ingestion_date}/"
            f"adscribe_{start_date}_{end_date}.csv"
        )

        print(f"Uploading to S3 path: {s3_key}")

        # ------------------------------------------------
        # STEP 4: Download CSV and upload to S3
        # ------------------------------------------------
        with urllib.request.urlopen(download_url) as response:
            s3.upload_fileobj(response, BUCKET, s3_key)

        print("Upload completed successfully")

        # ------------------------------------------------
        # STEP 5: Return response
        # ------------------------------------------------
        return {
            "status": "success",
            "start_date": start_date,
            "end_date": end_date,
            "s3_path": f"s3://{BUCKET}/{s3_key}"
        }

    except Exception as e:

        print("Error occurred:", str(e))

        return {
            "status": "error",
            "message": str(e)
        }