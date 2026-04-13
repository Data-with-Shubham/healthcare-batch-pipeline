# ============================================================
# ingestion/schema_validator.py — Incoming File Schema Validator
# ============================================================
"""
Validates that raw CSV/JSON files arriving in GCS contain the
required columns before the Beam pipeline processes them.

Usage:
    python ingestion/schema_validator.py --date 2024-01-01
"""

import argparse
import csv
import io
import json
import sys
from pathlib import Path

from google.cloud import storage

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.config import PROJECT_ID, GCS_BUCKET, GCS_RAW_PREFIX
from utils.logger import get_logger

log = get_logger("schema_validator")

# ── Expected Schemas ─────────────────────────────────────────
REQUIRED_COLUMNS = {
    "admissions": {
        "admission_id", "patient_id", "doctor_id", "department_id",
        "treatment_id", "admission_date", "discharge_date",
    },
    "patients":    {"patient_id", "age", "gender", "city"},
    "doctors":     {"doctor_id", "name", "specialization"},
    "departments": {"department_id", "department_name"},
    "treatments":  {"treatment_id", "treatment_name", "treatment_cost"},
}


def _detect_file_type(filename: str) -> str | None:
    """Return the logical table name inferred from the filename."""
    name = filename.lower()
    for key in REQUIRED_COLUMNS:
        if key in name:
            return key
    return None


def validate_csv_blob(blob: storage.Blob) -> tuple[bool, str]:
    """Download and validate a single CSV blob. Returns (ok, message)."""
    content = blob.download_as_text(encoding="utf-8")
    reader  = csv.DictReader(io.StringIO(content))
    actual  = set(reader.fieldnames or [])

    file_type = _detect_file_type(blob.name)
    if file_type is None:
        return True, f"SKIP — unrecognised file type: {blob.name}"

    required = REQUIRED_COLUMNS[file_type]
    missing  = required - actual
    if missing:
        return False, f"FAIL — {blob.name}: missing columns {missing}"
    return True, f"OK — {blob.name}"


def validate_date_prefix(run_date: str) -> bool:
    """Validate all files under the date prefix. Returns True if all pass."""
    bucket_name = GCS_BUCKET.replace("gs://", "")
    client      = storage.Client(project=PROJECT_ID)
    bucket      = client.bucket(bucket_name)
    prefix      = f"{GCS_RAW_PREFIX}/{run_date}/"

    blobs = list(bucket.list_blobs(prefix=prefix))
    if not blobs:
        log.warning(f"No files found at gs://{bucket_name}/{prefix}")
        return False

    all_ok = True
    for blob in blobs:
        if not blob.name.endswith(".csv"):
            log.info(f"Skipping non-CSV: {blob.name}")
            continue
        ok, msg = validate_csv_blob(blob)
        if ok:
            log.info(msg)
        else:
            log.error(msg)
            all_ok = False

    return all_ok


def main():
    parser = argparse.ArgumentParser(description="Validate raw hospital file schemas")
    parser.add_argument("--date", required=True, help="Run date YYYY-MM-DD")
    args = parser.parse_args()

    log.info(f"Validating schemas for date={args.date}")
    passed = validate_date_prefix(args.date)

    if passed:
        log.info("Schema validation passed ✓")
    else:
        log.error("Schema validation FAILED — aborting pipeline")
        sys.exit(1)


if __name__ == "__main__":
    main()
