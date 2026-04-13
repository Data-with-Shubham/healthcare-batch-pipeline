# ============================================================
# pipeline/main.py — Apache Beam Batch Pipeline Entry Point
# ============================================================
"""
Reads raw hospital CSV files from Cloud Storage, cleanses and
transforms them, then loads into BigQuery star-schema tables.

Run locally (DirectRunner):
    python pipeline/main.py --date 2024-01-01

Run on Dataflow:
    python pipeline/main.py --date 2024-01-01 --runner DataflowRunner
"""

import argparse
import csv
import io
import json
import sys
from datetime import date
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    GoogleCloudOptions,
    SetupOptions,
    StandardOptions,
)

sys.path.insert(0, str(Path(__file__).parent.parent))
from pipeline.transforms import (
    CleanAdmission, CleanPatient, CleanDoctor,
    CleanDepartment, CleanTreatment,
)
from pipeline.bq_writer import (
    write_fact_admissions, write_dim_patients, write_dim_doctors,
    write_dim_departments, write_dim_treatments,
)
from utils.config import (
    PROJECT_ID, REGION, GCS_BUCKET, GCS_RAW_PREFIX,
    DATAFLOW_TEMP_LOCATION, DATAFLOW_STAGING, DATAFLOW_JOB_NAME,
)
from utils.logger import get_logger

log = get_logger("beam_main")


# ── CSV → dict helper ─────────────────────────────────────────

class ParseCSV(beam.DoFn):
    """Parse a raw CSV text line into a dict using the header row."""

    def __init__(self, header: list[str]):
        self.header = header

    def process(self, element: str):
        try:
            reader = csv.DictReader(io.StringIO(element), fieldnames=self.header)
            for row in reader:
                yield dict(row)
        except Exception as exc:
            log.error(f"CSV parse error: {exc} | line={element[:200]}")


# ── Pipeline Builder ──────────────────────────────────────────

def _gcs_pattern(run_date: str, filename: str) -> str:
    return f"{GCS_BUCKET}/{GCS_RAW_PREFIX}/{run_date}/{filename}"


def build_pipeline(pipeline: beam.Pipeline, run_date: str):
    """Wire all sources, transforms, and BigQuery sinks."""

    def read_csv(label: str, filename: str, header: list[str]):
        return (
            pipeline
            | f"Read{label}"  >> beam.io.ReadFromText(_gcs_pattern(run_date, filename), skip_header_lines=1)
            | f"Parse{label}" >> beam.ParDo(ParseCSV(header))
        )

    # ── 1. Dimension tables ───────────────────────────────────
    patients = (
        read_csv("Patients", "patients.csv",
                 ["patient_id", "age", "gender", "city"])
        | "CleanPatients" >> beam.ParDo(CleanPatient())
    )
    write_dim_patients(patients)

    doctors = (
        read_csv("Doctors", "doctors.csv",
                 ["doctor_id", "name", "specialization"])
        | "CleanDoctors" >> beam.ParDo(CleanDoctor())
    )
    write_dim_doctors(doctors)

    departments = (
        read_csv("Departments", "departments.csv",
                 ["department_id", "department_name"])
        | "CleanDepartments" >> beam.ParDo(CleanDepartment())
    )
    write_dim_departments(departments)

    treatments = (
        read_csv("Treatments", "treatments.csv",
                 ["treatment_id", "treatment_name", "treatment_cost"])
        | "CleanTreatments" >> beam.ParDo(CleanTreatment())
    )
    write_dim_treatments(treatments)

    # ── 2. Fact table ─────────────────────────────────────────
    validated = (
        read_csv("Admissions", "admissions.csv",
                 ["admission_id", "patient_id", "doctor_id", "department_id",
                  "treatment_id", "admission_date", "discharge_date", "readmission_flag"])
        | "CleanAdmissions" >> beam.ParDo(CleanAdmission()).with_outputs(
            CleanAdmission.VALID, CleanAdmission.INVALID
        )
    )

    write_fact_admissions(validated[CleanAdmission.VALID])

    # Dead-letter: log invalid rows (extend to write to GCS for reprocessing)
    (
        validated[CleanAdmission.INVALID]
        | "LogInvalidAdmissions" >> beam.Map(
            lambda r: log.warning(f"Dead-lettered admission: {r}")
        )
    )

    return pipeline


# ── Pipeline Options ──────────────────────────────────────────

def get_options(runner: str) -> PipelineOptions:
    options = PipelineOptions()

    options.view_as(StandardOptions).runner = runner

    gcp = options.view_as(GoogleCloudOptions)
    gcp.project          = PROJECT_ID
    gcp.region           = REGION
    gcp.temp_location    = DATAFLOW_TEMP_LOCATION
    gcp.staging_location = DATAFLOW_STAGING
    gcp.job_name         = DATAFLOW_JOB_NAME

    options.view_as(SetupOptions).save_main_session = True
    return options


# ── Entry Point ───────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Healthcare batch Beam pipeline")
    parser.add_argument("--date",   default=str(date.today()), help="Run date YYYY-MM-DD")
    parser.add_argument("--runner", default="DirectRunner",
                        choices=["DirectRunner", "DataflowRunner"])
    args, _ = parser.parse_known_args()

    log.info(f"Pipeline starting — date={args.date}, runner={args.runner}")
    options = get_options(runner=args.runner)

    with beam.Pipeline(options=options) as p:
        build_pipeline(p, run_date=args.date)

    log.info("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
