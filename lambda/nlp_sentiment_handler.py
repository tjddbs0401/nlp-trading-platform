"""
Lambda: nlp-sentiment-processor

Trigger:
- S3 ObjectCreated (raw/text/)

Purpose:
- Create sentiment analysis jobs in DynamoDB
- Decouple ingestion from ML inference

Downstream:
- EC2 FinBERT worker (polls DynamoDB)

Runtime:
- Python 3.12 | x86_64
"""

import boto3, json, urllib.parse
from datetime import datetime

ddb = boto3.resource("dynamodb")
table = ddb.Table("JobState")

def handler(event, context):
    """
    Triggered by new S3 objects.
    Adds a 'PENDING_SENTIMENT' job to DynamoDB for the EC2 worker.
    Ensures key uses correct folder structure (e.g., raw/text/YYYY/MM/DD/file.jsonl.gz)
    """
    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        # decode URL-encoded keys (e.g., spaces or %2F)
        key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])

        # only accept .jsonl.gz files under raw/text/
        if not key.endswith(".gz") or not key.startswith("raw/text/"):
            print(f"⚠️ Skipping non-target file: {key}")
            continue

        timestamp = datetime.utcnow()
        pk_value = f"JOB#sentiment#{timestamp:%Y%m%d%H%M%S}"

        job_item = {
            "pk": pk_value,
            "sk": key,  # ✅ full actual S3 key from the event
            "status": "PENDING_SENTIMENT",
            "bucket": bucket,
            "created_ts": int(timestamp.timestamp()),
        }

        table.put_item(Item=job_item)
        print(f"📋 Queued sentiment job for s3://{bucket}/{key}")

    return {"statusCode": 200, "message": "Jobs queued successfully."}
