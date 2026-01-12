"""
Lambda: kinesis-to-s3

Trigger:
- Amazon Kinesis Data Stream

Purpose:
- Ingest streaming text data
- Persist raw records to S3 data lake (raw/text/)

Runtime:
- Python 3.12 | x86_64
"""

import boto3, json, gzip, base64
from io import BytesIO
from datetime import datetime

s3 = boto3.client("s3")
BUCKET = "nlp-trading-platform"

def handler(event, context):
    """
    Lambda triggered by Kinesis stream events.
    It writes the received records to S3 as gzipped JSONL files.
    """
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for record in event["Records"]:
            # Decode and parse each record
            payload = base64.b64decode(record["kinesis"]["data"])
            gz.write(payload + b"\n")

    key = f"raw/text/{datetime.utcnow():%Y/%m/%d/%H%M%S}.jsonl.gz"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/gzip"
    )

    print(f"✅ Saved {len(event['Records'])} records to s3://{BUCKET}/{key}")
    return {"statusCode": 200, "body": f"Saved to {key}"}
