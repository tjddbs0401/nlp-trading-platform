# EC2 FinBERT Worker

This directory contains the long-running **EC2-based worker** responsible for
heavy ML inference using **FinBERT**.  
The worker is intentionally separated from AWS Lambda to avoid Lambda timeout
and memory constraints, and to support scalable batch processing.

---

## What this worker does

**Core responsibilities**
- Polls DynamoDB for jobs with status `PENDING_SENTIMENT`
- Downloads raw JSONL (`.jsonl` / `.jsonl.gz`) inputs from S3 (`raw/text/`)
- Runs FinBERT sentiment inference on each record
- Writes sentiment outputs back to S3 (`curated/sentiment/`)
- Updates DynamoDB job status (`DONE` / `FAILED`)
- Emits logs to CloudWatch for observability

---

## Files

### `finbert_worker.py`
Main worker entrypoint.
- DynamoDB polling loop
- S3 download and upload
- FinBERT batch inference
- Job status transitions
- Error handling and logging

---

## Job lifecycle (DynamoDB)

Typical status flow:

`PENDING_SENTIMENT` → `DONE`  
Failures move to: `FAILED` (with optional error message)

The worker scans DynamoDB for pending jobs and processes them sequentially.
Jobs are marked as `FAILED` if S3 access, parsing, or inference fails.

---

## DynamoDB schema (as used by the worker)

The worker assumes the following DynamoDB item structure:

**Primary keys**
- `pk` — partition key (job identifier)
- `sk` — sort key (full S3 object key of the raw input file)

**Attributes**
- `bucket` — S3 bucket name containing the raw input
- `status` — `PENDING_SENTIMENT | DONE | FAILED`
- `error_message` — optional, populated on failure
---

## Operational notes

**Why EC2 for inference?**
- FinBERT requires heavier runtime (torch/transformers) and longer execution time
- EC2 allows flexible compute sizing and avoids Lambda constraints
- Worker can batch jobs and reuse a warm model in memory

**Logging**
- Worker should log: job_id, s3_key, record_count, latency, failures
- CloudWatch logs are used to verify successful end-to-end runs

---

## How to run (example)

On EC2:
1) Install dependencies (see root `requirements.txt`)
Finbert should be downloaded(HuggingFace cache)/internet connection needed
```
AutoTokenizer.from_pretrained("yiyanghkust/finbert-tone")
AutoModelForSequenceClassification.from_pretrained("yiyanghkust/finbert-tone")
```
2) Configure AWS credentials/role with access to S3 + DynamoDB
3) Start worker:
   - `python finbert_worker.py`
   - or use `systemd_worker.service`
