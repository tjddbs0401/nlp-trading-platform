#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FinBERT worker: scans DynamoDB for PENDING_SENTIMENT jobs, reads raw S3 JSONL(.gz),
scores with FinBERT, writes curated output, and marks jobs DONE.

Table: JobState (pk, sk, bucket, status, created_ts)
- sk: full S3 key, e.g. raw/text/2025/11/09/testfile.jsonl.gz
- bucket: S3 bucket name, e.g. nlp-trading-platform
- status: PENDING_SENTIMENT | DONE | FAILED
"""

# ---------- noise control (keep CloudWatch clean) ----------
import warnings, sys, os, json, time, gzip
warnings.filterwarnings("ignore")
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

# ---------- threading / perf ----------
import multiprocessing as mp
import boto3
from botocore.exceptions import ClientError
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from io import BytesIO

# ---------- config ----------
REGION = os.environ.get("AWS_REGION", "us-east-1")
DDB_TABLE = os.environ.get("JOBSTATE_TABLE", "JobState")
RAW_PREFIX = "raw/text/"
CURATED_PREFIX = "curated/sentiment/"
MODEL_NAME = os.environ.get("FINBERT_MODEL", "yiyanghkust/finbert-tone")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "32"))
MAX_LEN = int(os.environ.get("MAX_LEN", "96"))
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "60"))

# ---------- boto3 clients (use instance role creds) ----------
s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)

# ---------- model load (once) ----------
def load_finbert():
    print("Loading FinBERT...")
    # use all CPU cores
    try:
        torch.set_num_threads(max(1, mp.cpu_count()))
    except Exception:
        pass
    tok = AutoTokenizer.from_pretrained(MODEL_NAME)
    mdl = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
    mdl.eval()
    print("FinBERT loaded")
    return tok, mdl

TOKENIZER, MODEL = load_finbert()
LABELS = ["positive", "negative", "neutral"]  # order for finbert-tone

# ---------- helpers ----------
def ddb_to_py(item):
    """Convert a minimal DynamoDB item (string/number attrs) to dict[str, str]."""
    out = {}
    for k, v in item.items():
        if "S" in v:
            out[k] = v["S"]
        elif "N" in v:
            out[k] = v["N"]
        else:
            # extend as needed (L, M, etc.) — not needed for this worker
            out[k] = None
    return out

def update_status(pk, sk, status, msg=None):
    expr = "SET #s = :s"
    names = {"#s": "status"}
    vals = {":s": {"S": status}}
    if msg:
        expr += ", error_message = :e"
        vals[":e"] = {"S": msg[:500]}  # keep it short
    ddb.update_item(
        TableName=DDB_TABLE,
        Key={"pk": {"S": pk}, "sk": {"S": sk}},
        UpdateExpression=expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=vals,
    )

def list_objects_all(bucket, prefix, max_keys=1000):
    """List up to max_keys objects for a prefix (single page is fine for our worker)."""
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)
        return [c["Key"] for c in resp.get("Contents", [])]
    except ClientError:
        return []

def smart_get_object(bucket, key):
    """
    Fetch S3 object:
    1) Try the exact key
    2) If NoSuchKey and key looks undated (missing /YYYY/MM/DD/),
       try to locate it under RAW_PREFIX/**/filename by listing.
    Returns bytes (the body).
    """
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code != "NoSuchKey":
            raise

        # fallback: search for file by filename under RAW_PREFIX
        fname = key.split("/")[-1]
        search_prefix = RAW_PREFIX  # e.g., "raw/text/"
        print(f"⚠️ File not at exact key. Searching for '{fname}' under s3://{bucket}/{search_prefix} ...")
        candidates = list_objects_all(bucket, search_prefix)
        # look for a key that ends with the filename (handles date partitions)
        matches = [k for k in candidates if k.endswith("/" + fname) or k.endswith(fname)]
        if not matches:
            raise  # propagate original NoSuchKey

        # pick the most recent-looking (longest path / last in list)
        chosen = sorted(matches)[-1]
        print(f"➡️ Resolved to: s3://{bucket}/{chosen}")
        obj = s3.get_object(Bucket=bucket, Key=chosen)
        return obj["Body"].read()

def read_jsonl_bytes(data: bytes, key: str):
    """Return list[dict] from JSONL bytes. Handles .gz automatically."""
    if key.endswith(".gz"):
        with gzip.GzipFile(fileobj=BytesIO(data)) as gz:
            text = gz.read().decode("utf-8", errors="ignore")
    else:
        text = data.decode("utf-8", errors="ignore")
    records = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            records.append(rec)
        except json.JSONDecodeError:
            # skip bad lines but keep processing
            continue
    return records

def run_finbert_batch(texts):
    with torch.no_grad():
        enc = TOKENIZER(texts, return_tensors="pt", truncation=True, padding=True, max_length=MAX_LEN)
        logits = MODEL(**enc).logits
        probs = torch.nn.functional.softmax(logits, dim=-1).tolist()
        preds = logits.argmax(dim=-1).tolist()
        return [{"label": LABELS[i], "probs": p} for i, p in zip(preds, probs)]


def write_curated(bucket, raw_key, enriched_records):
    """
    Write results as JSONL to curated/sentiment/YYYY/MM/DD/<basename>.jsonl
    Mirrors the folder structure from raw/text/.
    """
    # Derive the date subpath from the raw key if present
    # raw/text/2025/11/10/test_texts.jsonl.gz -> 2025/11/10/
    parts = raw_key.split("/")
    date_parts = []
    if len(parts) >= 6:  # raw, text, YYYY, MM, DD, file
        date_parts = parts[2:5]

    # Extract clean file base name
    base = parts[-1]
    if base.endswith(".gz"):
        base = base[:-3]
    if not base.endswith(".jsonl"):
        base = base + ".jsonl"

    # Build mirrored curated key
    if date_parts:
        out_key = f"{CURATED_PREFIX}{'/'.join(date_parts)}/{base}"
    else:
        out_key = f"{CURATED_PREFIX}{base}"

    # Write JSONL
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in enriched_records).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=out_key, Body=body)

    print(f"✅ Uploaded sentiment file: s3://{bucket}/{out_key}")
    return out_key


def process_job(item):
    d = ddb_to_py(item)
    pk = d["pk"]
    sk = d["sk"]
    bucket = d.get("bucket")

    print(f"🧾 Processing job: {sk}")

    # fetch & parse raw file
    try:
        raw_bytes = smart_get_object(bucket, sk)
    except ClientError as e:
        msg = f"NoSuchKey or S3 error for s3://{bucket}/{sk}: {e.response.get('Error', {}).get('Message', str(e))}"
        print(f"❌ {msg}")
        update_status(pk, sk, "FAILED", msg=msg)
        return

    records = read_jsonl_bytes(raw_bytes, sk)
    if not records:
        msg = "No valid JSONL records found."
        print(f"❌ {msg}")
        update_status(pk, sk, "FAILED", msg=msg)
        return

    # run finbert in batches
    headlines = [r.get("headline", "") or r.get("text", "") for r in records]
    symbols = [r.get("symbol") for r in records]
    enriched = []
    for i in range(0, len(headlines), BATCH_SIZE):
        batch = headlines[i:i+BATCH_SIZE]
        labels = run_finbert_batch(batch)
        for j, label in enumerate(labels):
            idx = i + j
            enriched.append({
                "symbol": symbols[idx],
                "headline": headlines[idx],
                "sentiment_label": label,
            })

    # write curated
    out_key = write_curated(bucket, sk, enriched)

    # mark done
    update_status(pk, sk, "DONE")
    print("📦 Marked job as DONE")

def poll_once():
    """Scan all pending jobs and process them."""
    try:
        resp = ddb.scan(
            TableName=DDB_TABLE,
            FilterExpression="#status = :s",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":s": {"S": "PENDING_SENTIMENT"}},
        )
        items = resp.get("Items", [])
        print(f"🔎 Found {len(items)} pending job(s)")
        for it in items:
            try:
                process_job(it)
            except Exception as e:
                # keep the worker alive if a job blows up
                print(f"❌ Error processing job: {e}")
                # best-effort mark FAILED with message
                d = ddb_to_py(it)
                try:
                    update_status(d["pk"], d["sk"], "FAILED", msg=str(e))
                except Exception:
                    pass
    except Exception as e:
        print(f"❌ Error scanning DynamoDB: {e}")

def main():
    while True:
        poll_once()
        print(f"⏳ Sleeping {POLL_SECONDS}s...")
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
