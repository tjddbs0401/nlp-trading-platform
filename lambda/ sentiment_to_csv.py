"""
Lambda: sentiment_to_csv

Trigger:
- S3 ObjectCreated (curated/sentiment/)

Purpose:
- Aggregate sentiment JSONL files into daily CSV features
- Prepare analytics-ready datasets for quant modeling

Runtime:
- Python 3.12 | x86_64
"""
import boto3
import gzip
import io
import json
import pandas as pd
from datetime import datetime

# -------- CONFIG --------
BUCKET = "nlp-trading-platform"
SENTIMENT_PREFIX = "curated/sentiment/"
OUTPUT_PREFIX = "curated/analytics/daily/"
s3 = boto3.client("s3")

# -------- HELPERS --------
def list_s3_files(prefix):
    """List all S3 objects under a given prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]

def read_jsonl_from_s3(key):
    """Read a JSONL or JSONL.GZ file from S3 and return list of dicts."""
    resp = s3.get_object(Bucket=BUCKET, Key=key)
    body = resp["Body"].read()
    if key.endswith(".gz"):
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as f:
            text = f.read().decode("utf-8")
    else:
        text = body.decode("utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]

def extract_records(records):
    """Flatten nested sentiment data into rows with numeric sentiment scores."""
    out = []
    for r in records:
        try:
            sym = r["symbol"]
            label = r["sentiment_label"]["label"]
            probs = r["sentiment_label"]["probs"]
            score = probs[0] - probs[1]  # positive_prob - negative_prob
            out.append({
                "symbol": sym,
                "label": label,
                "positive_prob": probs[0],
                "negative_prob": probs[1],
                "neutral_prob": probs[2],
                "sentiment_score": score
            })
        except Exception:
            continue
    return out

def aggregate(records):
    """Aggregate per symbol and compute average sentiment metrics."""
    df = pd.DataFrame(records)
    if df.empty:
        return df
    grouped = (
        df.groupby("symbol")
        .agg(
            avg_sentiment_score=("sentiment_score", "mean"),
            avg_positive=("positive_prob", "mean"),
            avg_negative=("negative_prob", "mean"),
            avg_neutral=("neutral_prob", "mean"),
            positive_count=("label", lambda x: (x == "positive").sum()),
            negative_count=("label", lambda x: (x == "negative").sum()),
            neutral_count=("label", lambda x: (x == "neutral").sum()),
            total=("label", "count"),
        )
        .reset_index()
    )
    grouped["date"] = datetime.utcnow().strftime("%Y-%m-%d")
    return grouped

# -------- MAIN HANDLER --------
def lambda_handler(event, context):
    """
    Triggered by S3 'ObjectCreated' events when new sentiment JSONL files are uploaded.
    Aggregates all sentiment files from that same day into a summary CSV.
    """
    # Parse S3 event
    rec = event["Records"][0]
    bucket = rec["s3"]["bucket"]["name"]
    key = rec["s3"]["object"]["key"]

    # Extract date prefix from path: curated/sentiment/YYYY/MM/DD/
    parts = key.split("/")
    if len(parts) >= 5:
        date_prefix = "/".join(parts[2:5]) + "/"
    else:
        # fallback to today's UTC date
        date_prefix = datetime.utcnow().strftime("%Y/%m/%d/")

    prefix = f"{SENTIMENT_PREFIX}{date_prefix}"
    print(f"📂 Aggregating sentiment from: s3://{bucket}/{prefix}")

    # Collect all sentiment files for that date
    all_records = []
    for s3_key in list_s3_files(prefix):
        if not s3_key.endswith(".jsonl") and not s3_key.endswith(".jsonl.gz"):
            continue
        print(f"🔹 Reading {s3_key}")
        recs = read_jsonl_from_s3(s3_key)
        all_records.extend(extract_records(recs))

    if not all_records:
        print("⚠️ No sentiment records found for this date.")
        return {"status": "no_data"}

    # Aggregate results
    df = aggregate(all_records)
    csv_buf = io.StringIO()
    df.to_csv(csv_buf, index=False)

    # Write summary CSV to curated/analytics/daily/
    out_key = f"{OUTPUT_PREFIX}{datetime.utcnow():%Y/%m/%d/}sentiment_summary_{datetime.utcnow():%H%M%S}.csv"

    s3.put_object(Bucket=BUCKET, Key=out_key, Body=csv_buf.getvalue())

    print(f"✅ Uploaded summary: s3://{BUCKET}/{out_key}")
    return {"status": "ok", "rows": len(df)}
