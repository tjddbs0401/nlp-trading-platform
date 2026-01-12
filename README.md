# nlp-trading-platform
An event-driven AWS pipeline for financial news sentiment analysis using FinBERT. Ingests raw text from S3, orchestrates jobs with Lambda and DynamoDB, runs inference on EC2, and outputs structured sentiment data for quantitative trading research.

```mermaid
flowchart TB
  subgraph Ingestion
    A["Data Source"] -->|"raw JSONL.gz"| B["S3 raw/"]
  end

  subgraph Orchestration
    B -->|"S3 ObjectCreated Event"| C["Lambda #2<br/>nlp-sentiment-processor"]
    C -->|"Create/Update Job"| D["DynamoDB Job Table"]
  end

  subgraph Inference
    D -->|"Poll PENDING jobs"| E["EC2 FinBERT Worker"]
    E -->|"Write sentiment JSONL"| F["S3 curated/sentiment/"]
  end

  subgraph Analytics
    F -->|"S3 ObjectCreated Event"| G["Lambda #3<br/>sentiment_to_csv"]
    G -->|"Daily CSV / features"| H["S3 curated/analytics/<br/>+ feature_store/"]
    H --> I["Quant Analysis<br/>+ Signal Modeling"]
  end
```

## Repository structure

- **Lambda functions (serverless orchestration)**  
  → [lambda/README.md](lambda/README.md)

- **EC2 FinBERT worker (ML inference)**  
  → [worker/README.md](worker/README.md)

- **Infrastructure & setup notes**  
  → `infra/`

- **Example input/output samples**  
  → `example_data/`

- **Architecture diagrams & screenshots**  
  → `docs/`

## Repository Structure

- **Lambda functions**  
  → [lambda/README.md](lambda/README.md)

- **EC2 inference worker**  
  → [worker/README.md](worker/README.md)

- **Infrastructure & setup notes**  
  → `infra/`

- **Example input/output samples**  
  → `example_data/`

- **Screenshots & diagrams**  
  → `docs/`

---

## Core Components

### Lambda Layer (Serverless Orchestration)

- Handles event-driven triggers (S3, Kinesis)
- Creates and tracks jobs in DynamoDB
- Aggregates sentiment outputs into daily CSV features

➡️ **Details:** [lambda/README.md](lambda/README.md)

---

### EC2 FinBERT Worker (ML Inference)

- Polls DynamoDB for pending jobs
- Downloads raw inputs from S3
- Runs FinBERT sentiment inference
- Uploads JSONL outputs to S3 and updates job statuses

➡️ **Details:** [worker/README.md](worker/README.md)

---

## Outputs

### Sentiment JSONL (example)

```json
{"symbol":"AAPL","headline":"Apple stock rises after strong iPhone sales","sentiment_label":"positive"}
{"symbol":"TSLA","headline":"Tesla faces new regulatory challenges","sentiment_label":"negative"}
