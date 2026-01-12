# AWS Lambda Functions

This directory contains all AWS Lambda functions used for **event-driven orchestration**
in the NLP trading platform.  
Each function is responsible for a specific stage of ingestion, coordination, or
analytics preparation, while heavy ML inference is handled outside Lambda.

---

## Overview

The Lambda layer is designed to be **lightweight, stateless, and event-driven**.

- Lambdas react to **data arrival events** (S3, Kinesis)
- They coordinate work by creating jobs or aggregating outputs
- **No model inference runs inside Lambda** to avoid timeout and memory constraints
- DynamoDB and S3 are used to decouple stages of the pipeline

---

## Functions

### 1️⃣ nlp-sentiment-processor

**Trigger**
- Amazon S3 `ObjectCreated` event  
- Prefix: `raw/text/`

**Purpose**
- Creates sentiment analysis jobs in DynamoDB when new raw text data arrives
- Acts as the orchestration layer between data ingestion and ML inference
- Decouples serverless ingestion from EC2-based FinBERT processing

**Workflow**
1. A raw JSONL file is uploaded to `s3://<bucket>/raw/text/`
2. This Lambda function is triggered by the S3 event
3. A new job record is created in DynamoDB with status `PENDING_SENTIMENT`
4. An EC2-based FinBERT worker later polls DynamoDB and processes the job

**Downstream**
- DynamoDB (job queue and state tracking)
- EC2 FinBERT worker (sentiment inference)

---

### 2️⃣ sentiment_to_csv

**Trigger**
- Amazon S3 `ObjectCreated` event  
- Prefix: `curated/sentiment/`

**Purpose**
- Aggregates sentiment inference outputs produced by the EC2 worker
- Transforms raw sentiment JSONL files into daily CSV metrics
- Produces analytics-ready datasets for quantitative research and modeling

**Workflow**
1. A new sentiment JSONL file is written to `curated/sentiment/`
2. This Lambda function is triggered by the S3 event
3. Sentiment records are parsed and aggregated by symbol and date
4. Daily metrics are written to `curated/analytics/` or `feature_store/`

**Design Notes**
- Keeps aggregation logic serverless and cost-efficient
- Separates ML inference from downstream analytics and feature engineering
- Outputs are optimized for pandas-based analysis and backtesting

---

### 3️⃣ kinesis-to-s3

**Trigger**
- Amazon Kinesis Data Stream

**Purpose**
- Ingests real-time or near–real-time text data from streaming sources
- Persists incoming records to Amazon S3 in raw JSONL format
- Serves as the streaming ingestion layer of the pipeline

**Workflow**
1. Records are published to a Kinesis data stream
2. This Lambda function is triggered with batched stream records
3. Each record is decoded, parsed, and validated
4. Raw JSON objects are written to S3 under the `raw/text/` prefix
5. Downstream S3-triggered Lambdas handle further processing

**Design Notes**
- Enables scalable, low-latency ingestion
- Decouples streaming data sources from batch-oriented processing
- Allows both streaming and batch data to share the same downstream pipeline

---

## Design Philosophy

- **Event-driven**: All Lambdas react to data arrival, not schedules
- **Separation of concerns**:  
  - Lambda → orchestration & aggregation  
  - EC2 → heavy ML inference
- **Scalable and cost-efficient**: Serverless where possible, compute-heavy tasks isolated
- **Analytics-aware**: Outputs are designed for quantitative research and signal modeling
