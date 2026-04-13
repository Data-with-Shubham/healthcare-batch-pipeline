# ============================================================
# utils/config.py — GCP Project Configuration
# ============================================================

# ── Core GCP Settings ────────────────────────────────────────
PROJECT_ID   = "your-gcp-project-id"
REGION       = "us-central1"

# ── Cloud Storage ────────────────────────────────────────────
GCS_BUCKET       = "gs://your-raw-bucket"
GCS_RAW_PREFIX   = "hospital-records/raw"
GCS_ARCH_PREFIX  = "hospital-records/archive"

# ── BigQuery ─────────────────────────────────────────────────
BIGQUERY_DATASET  = "healthcare_dwh"
BIGQUERY_LOCATION = "US"

# Fully-qualified table references (project:dataset.table)
FACT_ADMISSIONS   = f"{PROJECT_ID}:{BIGQUERY_DATASET}.fact_admissions"
DIM_PATIENTS      = f"{PROJECT_ID}:{BIGQUERY_DATASET}.dim_patients"
DIM_DOCTORS       = f"{PROJECT_ID}:{BIGQUERY_DATASET}.dim_doctors"
DIM_DEPARTMENTS   = f"{PROJECT_ID}:{BIGQUERY_DATASET}.dim_departments"
DIM_TREATMENTS    = f"{PROJECT_ID}:{BIGQUERY_DATASET}.dim_treatments"

# ── Dataflow ─────────────────────────────────────────────────
DATAFLOW_TEMP_LOCATION = f"gs://your-raw-bucket/dataflow/temp"
DATAFLOW_STAGING       = f"gs://your-raw-bucket/dataflow/staging"
DATAFLOW_JOB_NAME      = "healthcare-batch-pipeline"

# ── BigQuery Schemas ─────────────────────────────────────────
FACT_ADMISSIONS_SCHEMA = ",".join([
    "admission_id:STRING",
    "patient_id:STRING",
    "doctor_id:STRING",
    "department_id:STRING",
    "treatment_id:STRING",
    "admission_date:DATE",
    "discharge_date:DATE",
    "length_of_stay:INTEGER",
    "readmission_flag:BOOLEAN",
    "ingested_at:TIMESTAMP",
])

DIM_PATIENTS_SCHEMA = ",".join([
    "patient_id:STRING",
    "age:INTEGER",
    "gender:STRING",
    "city:STRING",
])

DIM_DOCTORS_SCHEMA = ",".join([
    "doctor_id:STRING",
    "name:STRING",
    "specialization:STRING",
])

DIM_DEPARTMENTS_SCHEMA = ",".join([
    "department_id:STRING",
    "department_name:STRING",
])

DIM_TREATMENTS_SCHEMA = ",".join([
    "treatment_id:STRING",
    "treatment_name:STRING",
    "treatment_cost:FLOAT",
])
