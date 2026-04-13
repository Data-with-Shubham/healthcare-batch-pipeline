# ============================================================
# pipeline/transforms.py — Data Cleansing & Transformation Logic
# ============================================================

import re
from datetime import datetime, timezone, date

import apache_beam as beam

from utils.logger import get_logger

log = get_logger("transforms")


# ── Helpers ───────────────────────────────────────────────────

def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_date(value: str) -> str | None:
    """Try several common date formats; return ISO date string or None."""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(value.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return default


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return default


def _clean_str(value) -> str:
    return str(value).strip() if value is not None else ""


# ── DoFns ─────────────────────────────────────────────────────

class CleanAdmission(beam.DoFn):
    """
    Cleanse and enrich a raw admission record.
    Outputs tagged records: 'valid' or 'invalid'.
    """

    REQUIRED = {"admission_id", "patient_id", "doctor_id",
                 "department_id", "treatment_id", "admission_date", "discharge_date"}
    VALID   = "valid"
    INVALID = "invalid"

    def process(self, element: dict):
        missing = self.REQUIRED - set(element.keys())
        if missing:
            log.warning(f"Missing fields {missing} in admission record: {element.get('admission_id')}")
            yield beam.pvalue.TaggedOutput(self.INVALID, element)
            return

        adm_date = _parse_date(element.get("admission_date", ""))
        dis_date = _parse_date(element.get("discharge_date", ""))

        if not adm_date or not dis_date:
            log.warning(f"Invalid dates for admission_id={element.get('admission_id')}")
            yield beam.pvalue.TaggedOutput(self.INVALID, element)
            return

        adm_d = date.fromisoformat(adm_date)
        dis_d = date.fromisoformat(dis_date)
        los   = max((dis_d - adm_d).days, 0)

        cleaned = {
            "admission_id":    _clean_str(element["admission_id"]),
            "patient_id":      _clean_str(element["patient_id"]),
            "doctor_id":       _clean_str(element["doctor_id"]),
            "department_id":   _clean_str(element["department_id"]),
            "treatment_id":    _clean_str(element["treatment_id"]),
            "admission_date":  adm_date,
            "discharge_date":  dis_date,
            "length_of_stay":  los,
            "readmission_flag": element.get("readmission_flag", "false").lower() == "true",
            "ingested_at":     _utcnow(),
        }
        yield beam.pvalue.TaggedOutput(self.VALID, cleaned)


class CleanPatient(beam.DoFn):
    """Cleanse a raw patient dimension record."""

    def process(self, element: dict):
        yield {
            "patient_id": _clean_str(element.get("patient_id", "")),
            "age":        _safe_int(element.get("age", 0)),
            "gender":     _clean_str(element.get("gender", "Unknown")).capitalize(),
            "city":       _clean_str(element.get("city", "Unknown")).title(),
        }


class CleanDoctor(beam.DoFn):
    """Cleanse a raw doctor dimension record."""

    def process(self, element: dict):
        yield {
            "doctor_id":       _clean_str(element.get("doctor_id", "")),
            "name":            _clean_str(element.get("name", "")).title(),
            "specialization":  _clean_str(element.get("specialization", "General")).title(),
        }


class CleanDepartment(beam.DoFn):
    """Cleanse a raw department dimension record."""

    def process(self, element: dict):
        yield {
            "department_id":   _clean_str(element.get("department_id", "")),
            "department_name": _clean_str(element.get("department_name", "")).title(),
        }


class CleanTreatment(beam.DoFn):
    """Cleanse a raw treatment dimension record."""

    def process(self, element: dict):
        yield {
            "treatment_id":   _clean_str(element.get("treatment_id", "")),
            "treatment_name": _clean_str(element.get("treatment_name", "")).title(),
            "treatment_cost": _safe_float(element.get("treatment_cost", 0.0)),
        }
