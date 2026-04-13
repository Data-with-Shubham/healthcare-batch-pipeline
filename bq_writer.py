# ============================================================
# pipeline/bq_writer.py — BigQuery Write Configurations
# ============================================================

import apache_beam as beam
from apache_beam.io.gcp import bigquery as bq_io

from utils.config import (
    FACT_ADMISSIONS, FACT_ADMISSIONS_SCHEMA,
    DIM_PATIENTS,    DIM_PATIENTS_SCHEMA,
    DIM_DOCTORS,     DIM_DOCTORS_SCHEMA,
    DIM_DEPARTMENTS, DIM_DEPARTMENTS_SCHEMA,
    DIM_TREATMENTS,  DIM_TREATMENTS_SCHEMA,
)


def _bq_write(table: str, schema: str):
    """Return a WriteToBigQuery transform with standard batch settings."""
    return bq_io.WriteToBigQuery(
        table=table,
        schema=schema,
        write_disposition=bq_io.BigQueryDisposition.WRITE_TRUNCATE,   # full refresh each run
        create_disposition=bq_io.BigQueryDisposition.CREATE_IF_NEEDED,
        method="FILE_LOADS",                                           # batch — cost-efficient
    )


def write_fact_admissions(pcoll):
    return pcoll | "WriteFactAdmissions" >> _bq_write(FACT_ADMISSIONS, FACT_ADMISSIONS_SCHEMA)


def write_dim_patients(pcoll):
    return pcoll | "WriteDimPatients"    >> _bq_write(DIM_PATIENTS,    DIM_PATIENTS_SCHEMA)


def write_dim_doctors(pcoll):
    return pcoll | "WriteDimDoctors"     >> _bq_write(DIM_DOCTORS,     DIM_DOCTORS_SCHEMA)


def write_dim_departments(pcoll):
    return pcoll | "WriteDimDepartments" >> _bq_write(DIM_DEPARTMENTS, DIM_DEPARTMENTS_SCHEMA)


def write_dim_treatments(pcoll):
    return pcoll | "WriteDimTreatments"  >> _bq_write(DIM_TREATMENTS,  DIM_TREATMENTS_SCHEMA)
