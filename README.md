# Data Engineering Pipeline — ETL on AWS

An end-to-end data engineering pipeline built on AWS. Three AWS Glue jobs orchestrate the movement and transformation of data across S3, and load it into an RDS PostgreSQL database. Infrastructure is provisioned automatically via a Terraform-based orchestration system.

---

## Architecture

```
DummyJSON REST API
        │
        ▼
 [Glue Job 1] etl_api_to_csv_products
        │  Fetch → Transform → Enrich
        ▼
   S3 (products/processed/catalog.csv)
        │
        ▼
 [Glue Job 2] etl_csv_to_rds
        │  Read CSV → Clean → Load
        ▼
   RDS PostgreSQL (products_clean table)

   [Glue Job 3] etl_sales  (independent)
        │  Spark → generate numbers dataset
        ▼
   S3 (output/ CSV)
```

### AWS Services

| Service | Role |
|---|---|
| **S3** | Stores Glue scripts, job configs, and output data |
| **AWS Glue** | Runs the three ETL jobs (serverless Spark) |
| **RDS PostgreSQL** | Final analytical data store |

---

## ETL Jobs

### Job 1 — `etl_api_to_csv_products.py`
Nightly job that pulls the full product catalog from the [DummyJSON API](https://dummyjson.com/products), enriches each product with business labels, and writes a clean CSV to S3.

**Pipeline:** `DummyJSON API → pandas transform → S3 CSV`

Transformations applied:
- Drops image URLs, nested objects, and irrelevant metadata fields
- Derives `price_tier` (`budget` / `mid-range` / `premium`)
- Derives `rating_label` (`low` / `medium` / `high`)
- Adds `ingestion_timestamp` (UTC)

Config loaded from S3 at runtime (`--CONFIG_PATH`):
```json
{
  "API_URL": "https://dummyjson.com/products?limit=0",
  "OUTPUT_BUCKET_NAME": "your-output-bucket",
  "OUTPUT_PREFIX": "products/processed",
  "OUTPUT_FILE_NAME": "catalog.csv"
}
```

---

### Job 2 — `etl_csv_to_rds.py`
Reads the catalog CSV produced by Job 1 from S3, applies light cleansing, and loads it into RDS PostgreSQL.

**Pipeline:** `S3 CSV → pandas clean → RDS (append)`

Transformations applied:
- Normalises column names (lowercase, underscores)
- Strips whitespace from all string columns
- Adds `ingestion_timestamp` (UTC)

Config loaded from S3 at runtime (`--CONFIG_PATH`):
```json
{
  "INPUT_BUCKET_NAME": "databucket53",
  "INPUT_KEY_NAME": "products/catalog.csv",
  "DB_HOST": "...",
  "DB_PORT": "5432",
  "DB_NAME": "...",
  "DB_USER": "...",
  "DB_PASSWORD": "...",
  "DB_TABLE": "products_clean"
}
```

---

### Job 3 — `etl_sales.py`
Standalone Spark/Glue job that generates a sample numeric dataset and writes it as CSV to S3.

**Pipeline:** `Spark DataFrame (1–20) → S3 CSV`

Config loaded from S3 at runtime (`--CONFIG_PATH`):
```json
{
  "OUTPUT_BUCKET_NAME": "your-output-bucket"
}
```

---

## Project Structure

```
data-eng-test/
├── src/
│   └── jobs/
│       ├── etl_api_to_csv_products.py   # Job 1 — API → S3
│       ├── etl_csv_to_rds.py            # Job 2 — S3 → RDS
│       └── etl_sales.py                 # Job 3 — Spark → S3
├── tests/
│   ├── test_etl_prod.py                 # Tests for Job 1
│   ├── test_etl_csv_to_rds.py           # Tests for Job 2
│   └── test_etl_sales.py               # Tests for Job 3
├── requirements.txt                     # Runtime dependencies
├── requirements-dev.txt                 # Dev/test dependencies
└── .env.example                         # Environment variable reference
```

---

## Infrastructure

Infrastructure is **not committed here** — it is generated automatically by an AI orchestration system (Terraform + LangGraph agent) and pushed to this repo as part of the CI/CD provisioning flow.

Generated structure added at provisioning time:
```
bootstrap/          # Creates S3 backend bucket + DynamoDB lock table
infra/              # Main Terraform config (Glue, S3, RDS, IAM)
.github/workflows/  # CI/CD pipeline (bootstrap → infra apply → post-deploy)
```

---

## Local Development

### Prerequisites
- Python 3.11+
- AWS credentials configured (`~/.aws/credentials` or environment variables)

### Install dependencies
```bash
pip install -r requirements-dev.txt
```

### Run tests
```bash
pytest tests/ -v --cov=src
```

### Environment variables
Copy `.env.example` to `.env` and fill in the values:
```bash
cp .env.example .env
```

Key variables:

| Variable | Description |
|---|---|
| `OUTPUT_BUCKET_NAME` | S3 bucket for job outputs |
| `SCRIPTS_BUCKET_NAME` | S3 bucket where Glue scripts are uploaded |
| `DB_HOST` | RDS PostgreSQL endpoint |
| `DB_NAME` / `DB_USER` / `DB_PASSWORD` | RDS credentials |
| `GLUE_JOB_*_NAME` | Names of the deployed Glue jobs |

---

## CI/CD

The pipeline is generated and managed by the orchestration system. Once provisioned, it runs on every push to `main`:

1. **Bootstrap** — ensures the Terraform state backend (S3 + DynamoDB) exists
2. **Infra Apply** — runs `terraform apply` to provision all AWS resources
3. **Post-deploy** — uploads real Glue scripts to S3, replacing test artifacts

---

## Dependencies

**Runtime** (`requirements.txt`):
- `boto3` — AWS SDK
- `sqlalchemy` + `pg8000` — RDS PostgreSQL connector
- `requests` — HTTP client for the product API

**Dev/Test** (`requirements-dev.txt`):
- `pytest` + `pytest-cov` — test runner
- `moto[s3]` — AWS S3 mock for unit tests
- `pyspark` — local Spark for Glue job testing
- `pandas` + `numpy` — data transformation
