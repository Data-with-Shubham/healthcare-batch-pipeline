# ============================================================
# ingestion/gcs_uploader.py — Upload Raw Hospital Records to GCS
# ============================================================
"""
Uploads daily CSV/JSON hospital record files to Cloud Storage
under a date-partitioned prefix.

Usage:
    python ingestion/gcs_uploader.py --date 2024-01-01 --source ./data/
    python ingestion/gcs_uploader.py --date 2024-01-01 --source ./data/ --archive
"""

import argparse
import os
import sys
from datetime import date, datetime
from pathlib import Path

from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.config import PROJECT_ID, GCS_BUCKET, GCS_RAW_PREFIX, GCS_ARCH_PREFIX
from utils.logger import get_logger

log = get_logger("gcs_uploader")

SUPPORTED_EXTENSIONS = {".csv", ".json", ".jsonl"}


def upload_file(client: storage.Client, local_path: Path, gcs_path: str) -> bool:
    """Upload a single file to GCS. Returns True on success."""
    bucket_name = GCS_BUCKET.replace("gs://", "")
    try:
        bucket = client.bucket(bucket_name)
        blob   = bucket.blob(gcs_path)
        blob.upload_from_filename(str(local_path))
        log.info(f"Uploaded: {local_path.name} → gs://{bucket_name}/{gcs_path}")
        return True
    except GoogleAPIError as exc:
        log.error(f"Failed to upload {local_path.name}: {exc}")
        return False


def upload_directory(source_dir: Path, run_date: str, archive: bool = False) -> dict:
    """
    Upload all supported files from *source_dir* to GCS.
    Destination prefix: <GCS_RAW_PREFIX>/<run_date>/

    Returns a summary dict with counts.
    """
    client  = storage.Client(project=PROJECT_ID)
    files   = [f for f in source_dir.iterdir() if f.suffix in SUPPORTED_EXTENSIONS]

    if not files:
        log.warning(f"No supported files found in {source_dir}")
        return {"total": 0, "success": 0, "failed": 0}

    success, failed = 0, 0
    for f in files:
        gcs_path = f"{GCS_RAW_PREFIX}/{run_date}/{f.name}"
        if upload_file(client, f, gcs_path):
            success += 1
            if archive:
                arch_path = f"{GCS_ARCH_PREFIX}/{run_date}/{f.name}"
                upload_file(client, f, arch_path)
        else:
            failed += 1

    summary = {"total": len(files), "success": success, "failed": failed}
    log.info(f"Upload complete: {summary}")
    return summary


def main():
    parser = argparse.ArgumentParser(description="Upload hospital records to Cloud Storage")
    parser.add_argument("--date",    default=str(date.today()), help="Run date YYYY-MM-DD")
    parser.add_argument("--source",  default="./data",          help="Local source directory")
    parser.add_argument("--archive", action="store_true",        help="Also copy to archive prefix")
    args = parser.parse_args()

    source_dir = Path(args.source)
    if not source_dir.is_dir():
        log.error(f"Source directory not found: {source_dir}")
        sys.exit(1)

    log.info(f"Starting upload for date={args.date}, source={source_dir}")
    summary = upload_directory(source_dir, run_date=args.date, archive=args.archive)

    if summary["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
