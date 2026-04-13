# 🏥 Healthcare & Hospital Patient Analytics — Batch Data Pipeline

A production-style, end-to-end **batch data pipeline** built on **Google Cloud Platform (GCP)** for Hospital Patient Analytics.  
This pipeline extracts daily hospital records (admissions, diagnoses, treatments, discharge), processes them using **Apache Beam (Dataflow)** and **dbt**, loads into a **BigQuery star-schema** data warehouse, and powers **Power BI** dashboards for hospital management.

---

## 🏗️ Architecture

```
CSV / JSON Files (Hospital Records)
            ↓
     Cloud Storage (Raw Layer)
            ↓
   Dataflow (Apache Beam)
   [Cleansing + Transformation]
            ↓
        BigQuery
   ┌─────────────────────┐
   │  Star Schema DWH    │
   │  fact_admissions    │
   │  dim_patients       │
   │  dim_doctors        │
   │  dim_departments    │
   │  dim_treatments     │
   └─────────────────────┘
            ↓
       dbt (Data Quality)
            ↓
    Power BI Dashboards
            ↑
    Cloud Scheduler (Nightly Trigger)
    Cloud Logging  (Monitoring & Alerts)
```

---

## ⚙️ Tech Stack

| Layer              | Technology                    |
|--------------------|-------------------------------|
| Cloud Platform     | Google Cloud Platform (GCP)   |
| Raw Storage        | Cloud Storage (GCS)           |
| Batch Processing   | Dataflow (Apache Beam)        |
| Data Warehouse     | BigQuery                      |
| Data Transformation| dbt (Data Build Tool)         |
| Orchestration      | Cloud Scheduler               |
| Monitoring         | Cloud Logging + Alerting      |
| Visualization      | Power BI                      |
| Language           | Python 3.11+                  |

---

## 📁 Project Structure

```
healthcare-batch-pipeline/
│
├── ingestion/
│   ├── gcs_uploader.py          # Upload raw CSV/JSON files to Cloud Storage
│   └── schema_validator.py      # Validate incoming file schema before processing
│
├── pipeline/
│   ├── main.py                  # Apache Beam batch pipeline entry point
│   ├── transforms.py            # Data cleansing & transformation DoFns
│   └── bq_writer.py             # BigQuery write configurations
│
├── dbt/
│   ├── dbt_project.yml          # dbt project configuration
│   ├── profiles.yml             # BigQuery connection profiles (dev / prod)
│   └── models/
│       ├── schema.yml           # dbt tests & column documentation
│       ├── fact_admissions.sql  # Fact table — partitioned by month, clustered
│       ├── dim_patients.sql     # Patient dimension + age band derivation
│       ├── dim_doctors.sql      # Doctor dimension
│       ├── dim_departments.sql  # Department dimension
│       └── dim_treatments.sql   # Treatment dimension + cost band derivation
│
├── utils/
│   ├── config.py                # GCP project configuration & BQ schemas
│   └── logger.py                # Centralized JSON logging (Cloud Logging compatible)
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 📦 BigQuery Schema

### `fact_admissions` (Fact Table)

| Column           | Type      | Description                    |
|------------------|-----------|--------------------------------|
| admission_id     | STRING    | Unique admission ID (PK)       |
| patient_id       | STRING    | FK → dim_patients              |
| doctor_id        | STRING    | FK → dim_doctors               |
| department_id    | STRING    | FK → dim_departments           |
| treatment_id     | STRING    | FK → dim_treatments            |
| admission_date   | DATE      | Date of admission              |
| discharge_date   | DATE      | Date of discharge              |
| length_of_stay   | INTEGER   | Days in hospital               |
| readmission_flag | BOOLEAN   | Readmission within 30 days     |
| ingested_at      | TIMESTAMP | Pipeline load timestamp        |

### `dim_patients`

| Column     | Type    |
|------------|---------|
| patient_id | STRING  |
| age        | INTEGER |
| gender     | STRING  |
| city       | STRING  |

### `dim_doctors`

| Column         | Type   |
|----------------|--------|
| doctor_id      | STRING |
| name           | STRING |
| specialization | STRING |

### `dim_departments`

| Column          | Type   |
|-----------------|--------|
| department_id   | STRING |
| department_name | STRING |

### `dim_treatments`

| Column         | Type   |
|----------------|--------|
| treatment_id   | STRING |
| treatment_name | STRING |
| treatment_cost | FLOAT  |

---

## 📊 Pipeline Features

- ✅ **Batch ingestion** — processes 50,000+ daily hospital records
- ✅ **Star schema design** — 1 fact table + 4 dimension tables
- ✅ **Dead-letter handling** — invalid records logged separately, never silently dropped
- ✅ **dbt transformations** — data quality tests, deduplication, derived columns (age band, cost band, readmission recalculation)
- ✅ **Cloud Scheduler** — fully automated nightly runs (100% manual effort eliminated)
- ✅ **Cloud Logging** — structured JSON logs compatible with GCP Log Explorer
- ✅ **Auditable loads** — every run logged with row counts and status
- ✅ **Power BI dashboards** — hospital management reporting

---

## 🚀 How to Run

### 1. Clone the repo

```bash
git clone https://github.com/<your-username>/healthcare-batch-pipeline.git
cd healthcare-batch-pipeline
```

### 2. Create a virtual environment & install dependencies

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure GCP settings

Edit `utils/config.py`:

```python
PROJECT_ID       = "your-gcp-project-id"
GCS_BUCKET       = "gs://your-raw-bucket"
BIGQUERY_DATASET = "healthcare_dwh"
```

Authenticate with GCP:

```bash
gcloud auth application-default login
```

### 4. Create GCP resources (one-time)

```bash
# GCS bucket
gsutil mb gs://your-raw-bucket

# BigQuery dataset
bq mk --dataset --location=US your-gcp-project-id:healthcare_dwh

# Dataflow temp/staging buckets
gsutil mb gs://your-raw-bucket/dataflow
```

### 5. Validate & upload raw files

```bash
# Upload files for a specific date
python ingestion/gcs_uploader.py --date 2024-01-01 --source ./data/

# Validate schema before pipeline runs
python ingestion/schema_validator.py --date 2024-01-01
```

### 6. Run the Dataflow batch pipeline

```bash
# Local (DirectRunner) — for development/testing
python pipeline/main.py --date 2024-01-01

# Cloud Dataflow — production
python pipeline/main.py --date 2024-01-01 --runner DataflowRunner
```

### 7. Run dbt transformations & tests

```bash
cd dbt/
dbt run   --project-dir . --profiles-dir .
dbt test  --project-dir . --profiles-dir .
```

### 8. Schedule via Cloud Scheduler

```bash
gcloud scheduler jobs create http healthcare-nightly-pipeline \
  --schedule="0 1 * * *" \
  --uri="YOUR_CLOUD_FUNCTION_OR_CLOUD_RUN_URL" \
  --time-zone="UTC" \
  --location="us-central1"
```

---

## 📈 Analytics Use Cases

| Use Case                                  | Tables Used                          |
|-------------------------------------------|--------------------------------------|
| Average length of stay per department     | fact_admissions + dim_departments    |
| 30-day readmission rate tracking          | fact_admissions                      |
| Doctor-wise patient load analysis         | fact_admissions + dim_doctors        |
| Peak admission periods & seasonal trends  | fact_admissions                      |
| Treatment cost analysis by category       | fact_admissions + dim_treatments     |
| Department-wise occupancy reporting       | fact_admissions + dim_departments    |

---

## 👨‍💻 Author

**Shubham Wagh** — GCP Data Engineer | Python • SQL • BigQuery • Dataflow • dbt

- 🔗 [LinkedIn](https://linkedin.com/in/shubhamwagh)
- 💻 [GitHub](https://github.com/shubhamwagh)
- 📧 shubhamharishwagh@gmail.com

---

## 📄 License

MIT © 2024 — feel free to use, modify, and distribute.
