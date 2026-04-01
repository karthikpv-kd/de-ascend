import boto3
import json
import base64
import os
from datetime import datetime
from email.parser import BytesParser
from email.policy import default

s3 = boto3.client("s3")

# Environment variable
BUCKET_NAME = os.environ.get("BUCKET_NAME")


def lambda_handler(event, context):

    try:

        # -----------------------------
        # Step 0: Validate env vars
        # -----------------------------
        if not BUCKET_NAME:
            raise ValueError("Missing BUCKET_NAME environment variable")

        # -----------------------------
        # Step 1: Decode body correctly
        # -----------------------------
        if event.get("isBase64Encoded"):
            body = base64.b64decode(event["body"])
        else:
            body = event["body"].encode()

        # -----------------------------
        # Step 2: Get content-type safely
        # -----------------------------
        headers = event.get("headers", {})
        content_type = headers.get("content-type") or headers.get("Content-Type")

        if not content_type:
            return {
                "statusCode": 400,
                "body": json.dumps("Missing Content-Type header")
            }

        # -----------------------------
        # Step 3: Parse multipart
        # -----------------------------
        msg = BytesParser(policy=default).parsebytes(
            b"Content-Type: " + content_type.encode() + b"\r\n\r\n" + body
        )

        file_content = None
        file_name = None
        client = None
        dataset = None

        for part in msg.iter_parts():
            content_disposition = part.get("Content-Disposition", "")

            if 'name="file"' in content_disposition:
                file_content = part.get_payload(decode=True)
                file_name = part.get_filename()

            elif 'name="client"' in content_disposition:
                client = part.get_payload(decode=True).decode().strip()

            elif 'name="dataset"' in content_disposition:
                dataset = part.get_payload(decode=True).decode().strip()

        # -----------------------------
        # Step 4: Validation
        # -----------------------------
        if not file_content or not client or not dataset:
            return {
                "statusCode": 400,
                "body": json.dumps("Missing file/client/dataset")
            }

        # -----------------------------
        # Step 5: Build S3 path
        # -----------------------------
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        s3_key = f"bronze/source=upload/client={client}/dataset={dataset}/{timestamp}_{file_name}"

        print(f"Uploading to: s3://{BUCKET_NAME}/{s3_key}")

        # -----------------------------
        # Step 6: Upload to S3
        # -----------------------------
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=file_content
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "File uploaded successfully",
                "s3_path": f"s3://{BUCKET_NAME}/{s3_key}"
            })
        }

    except Exception as e:

        print("Error:", str(e))

        return {
            "statusCode": 500,
            "body": json.dumps(str(e))
        }