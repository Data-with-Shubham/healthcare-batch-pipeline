# 🏥 Healthcare & Hospital Patient Analytics — Batch Data Pipeline

A production-style, end-to-end **batch data pipeline** built on **Google Cloud Platform (GCP)** for Hospital Patient Analytics. This pipeline extracts daily hospital records (admissions, diagnoses, treatments, discharge), processes and transforms them using Apache Beam (Dataflow) and dbt, loads into a BigQuery star-schema data warehouse, and powers Power BI dashboards for hospital management.

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
    Cloud Logging (Monitoring & Alerts)
```

---

## ⚙️ Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Google Cloud Platform (GCP) |
| Raw Storage | Cloud Storage (GCS) |
| Batch Processing | Dataflow (Apache Beam) |
| Data Warehouse | BigQuery |
| Data Transformation | dbt (Data Build Tool) |
| Orchestration | Cloud Scheduler |
| Monitoring | Cloud Logging + Alerting |
| Visualization | Power BI |
| Language | Python 3.x |

---

## 📁 Project Structure

```
healthcare-batch-pipeline/
│
├── ingestion/
│   ├── gcs_uploader.py          # Upload raw CSV/JSON files to Cloud Storage
│   └── schema_validator.py      # Validate incoming file schema
│
├── pipeline/
│   ├── main.py                  # Apache Beam batch pipeline entry point
│   ├── transforms.py            # Data cleansing & transformation logic
│   └── bq_writer.py             # BigQuery write configurations
│
├── dbt/
│   ├── models/
│   │   ├── fact_admissions.sql  # Fact table model
│   │   ├── dim_patients.sql     # Patient dimension
│   │   ├── dim_doctors.sql      # Doctor dimension
│   │   ├── dim_departments.sql  # Department dimension
│   │   └── dim_treatments.sql   # Treatment dimension
│   └── dbt_project.yml
│
├── utils/
│   ├── config.py                # GCP project configuration
│   └── logger.py                # Centralized logging setup
│
└── README.md
```

---

## 📊 Pipeline Features

- ✅ **Batch ingestion** — processes 50,000+ daily hospital records
- ✅ **Star schema design** — 1 fact table + 4 dimension tables
- ✅ **dbt transformations** — data quality checks & validation
- ✅ **Cloud Scheduler** — fully automated nightly runs (100% manual effort eliminated)
- ✅ **Cloud Logging** — monitoring with alerting for pipeline health
- ✅ **Auditable loads** — every run logged with row counts and status
- ✅ **Power BI dashboards** — hospital management reporting

---

## 📦 BigQuery Schema

### `fact_admissions` (Fact Table)
| Column | Type | Description |
|---|---|---|
| admission_id | STRING | Unique admission ID |
| patient_id | STRING | FK → dim_patients |
| doctor_id | STRING | FK → dim_doctors |
| department_id | STRING | FK → dim_departments |
| treatment_id | STRING | FK → dim_treatments |
| admission_date | DATE | Date of admission |
| discharge_date | DATE | Date of discharge |
| length_of_stay | INTEGER | Days in hospital |
| readmission_flag | BOOLEAN | Readmission within 30 days |
| ingested_at | TIMESTAMP | Pipeline load timestamp |

### `dim_patients`
| Column | Type |
|---|---|
| patient_id | STRING |
| age | INTEGER |
| gender | STRING |
| city | STRING |

### `dim_doctors`
| Column | Type |
|---|---|
| doctor_id | STRING |
| name | STRING |
| specialization | STRING |

### `dim_departments`
| Column | Type |
|---|---|
| department_id | STRING |
| department_name | STRING |

### `dim_treatments`
| Column | Type |
|---|---|
| treatment_id | STRING |
| treatment_name | STRING |
| treatment_cost | FLOAT |

---

## 🚀 How to Run

### 1. Install dependencies
```bash
pip install apache-beam[gcp] google-cloud-storage dbt-bigquery
```

### 2. Configure GCP settings
Edit `utils/config.py`:
```python
PROJECT_ID = "your-gcp-project-id"
GCS_BUCKET = "gs://your-raw-bucket/hospital-records/"
BIGQUERY_DATASET = "healthcare_dwh"
```

### 3. Upload raw files to Cloud Storage
```bash
python ingestion/gcs_uploader.py --date 2024-01-01
```

### 4. Run the Dataflow batch pipeline
```bash
python pipeline/main.py
```

### 5. Run dbt transformations
```bash
dbt run --project-dir dbt/
dbt test --project-dir dbt/
```

### 6. Schedule via Cloud Scheduler
```bash
gcloud scheduler jobs create http healthcare-nightly-pipeline \
  --schedule="0 1 * * *" \
  --uri="YOUR_CLOUD_FUNCTION_URL"
```

---

## 📈 Analytics Use Cases

- 🏥 Average length of stay per department
- 🔁 30-day readmission rate tracking
- 👨‍⚕️ Doctor-wise patient load analysis
- 📅 Peak admission periods & seasonal trends
- 💊 Treatment cost analysis by category
- 🏢 Department-wise occupancy reporting

---

## 👨‍💻 Author

**Shubham Wagh**
GCP Data Engineer | Python • SQL • BigQuery • Dataflow • dbt

- 🔗 [LinkedIn](https://www.linkedin.com/in/shubhamwagh-b2b9192a8)
- 💻 [GitHub](https://github.com/Data-with-Shubham)
- 📧 shubhamharishwagh@gmail.com

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).
