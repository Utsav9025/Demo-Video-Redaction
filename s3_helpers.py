import boto3
import os
import json
import logging

INPUT_BUCKET = os.getenv("S3_INPUT_BUCKET", "video-input-bucket")
OUTPUT_BUCKET = os.getenv("S3_OUTPUT_BUCKET", "video-output-bucket")

logger = logging.getLogger("face_service")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localstack:4566"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    )

def download_from_s3(s3_key: str, local_path: str):
    """Download a file from the INPUT bucket to a local path."""
    s3 = get_s3_client()
    logger.debug(f"Downloading s3://{INPUT_BUCKET}/{s3_key} -> {local_path}")
    s3.download_file(INPUT_BUCKET, s3_key, local_path)

def upload_json_to_s3(data: dict, s3_key: str):
    """Upload a JSON dict directly to the OUTPUT bucket."""
    s3 = get_s3_client()
    s3.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=s3_key,
        Body=json.dumps(data, indent=4),
        ContentType="application/json"
    )
    logger.info(f"Uploaded JSON to s3://{OUTPUT_BUCKET}/{s3_key}")
