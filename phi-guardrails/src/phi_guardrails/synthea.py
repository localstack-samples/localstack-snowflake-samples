"""The pinned Synthea extract this sample loads, and the rows derived from it.

Source: the Synthea "1K sample" CSV export, April 2020 build, published by the
Synthea project (MITRE) at synthetichealth.github.io. Synthea data is fully
synthetic: no real patient is described. The generator is Apache-2.0 licensed
and the project states its output "can be used without concern for legal or
privacy restrictions". The archive is downloaded at run time and verified by
SHA-256, never committed.

The extract keeps three hospitals, their encounters since 2010, and the
patients seen there. This module is the single place that turns the CSVs into
table rows, so the loader and the test oracle can never disagree about the
input data. The oracle's expected values come from these rows and the access
contract, never from querying the policies under test.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
import unicodedata
import urllib.request
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from functools import cache
from pathlib import Path

from phi_guardrails.settings import SAMPLE_ROOT

DATASET_URL = "https://synthetichealth.github.io/synthea-sample-data/downloads/" "synthea_sample_data_csv_apr2020.zip"
DATASET_SHA256 = "4194b18c11eaedcf0d5d5dd448d8ac9661f14381e2ef9f109215dc42266cd38a"
CACHE_PATH = SAMPLE_ROOT / ".cache" / "synthea_sample_data_csv_apr2020.zip"

# Facility id -> Synthea organization id. The ids are the sample's own stable
# keys; facility roles are named after them (FACILITY_101, ...).
FACILITY_ORGANIZATIONS = {
    101: "d692e283-0833-3201-8e55-4f868a9c0736",  # HALLMARK HEALTH SYSTEM, Melrose
    102: "e002090d-4e92-300e-b41e-7d1f21dee4c6",  # CAMBRIDGE HEALTH ALLIANCE, Cambridge
    103: "3d10019f-c88e-3de5-9916-6107b9c0263d",  # NEWTON-WELLESLEY HOSPITAL, Newton
}
ENCOUNTERS_SINCE = "2010-01-01"


@dataclass(frozen=True)
class Dataset:
    facilities: list[dict]
    patients: list[dict]
    encounters: list[dict]

    def table(self, name: str) -> list[dict]:
        return {
            "CLINICAL.FACILITIES": self.facilities,
            "CLINICAL.PATIENTS": self.patients,
            "CLINICAL.ENCOUNTERS": self.encounters,
        }[name]


def download(path: Path = CACHE_PATH) -> Path:
    """Fetch the archive once into ``.cache/`` and verify its checksum on every use."""
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_suffix(".part")
        with urllib.request.urlopen(DATASET_URL, timeout=120) as response:  # noqa: S310 (pinned https URL)
            partial.write_bytes(response.read())
        partial.replace(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != DATASET_SHA256:
        raise RuntimeError(f"{path} has SHA-256 {digest}, expected {DATASET_SHA256}. Delete it and retry.")
    return path


def _read(archive: zipfile.ZipFile, name: str) -> list[dict]:
    with archive.open(f"csv/{name}") as raw:
        return list(csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8")))


def _text(value: str) -> str | None:
    return value.strip() or None


def _date(value: str) -> date | None:
    return date.fromisoformat(value) if value else None


def _timestamp(value: str) -> datetime:
    # Synthea writes UTC instants ("2010-01-23T17:45:28Z"); the table stores TIMESTAMP_NTZ in UTC.
    return datetime.fromisoformat(value.removesuffix("Z"))


def _name_part(value: str) -> str:
    # Synthea appends digits to names ("Milo271"); emails use the plain ASCII letters.
    ascii_only = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z]", "", ascii_only.lower())


def patient_email(first: str, last: str) -> str:
    """Synthetic contact address backfilled into the EMAIL column added by V1.3.0."""
    return f"{_name_part(first)}.{_name_part(last)}@example.org"


@cache
def load_dataset() -> Dataset:
    with zipfile.ZipFile(download()) as archive:
        organizations = {row["Id"]: row for row in _read(archive, "organizations.csv")}
        facility_of = {org: fid for fid, org in FACILITY_ORGANIZATIONS.items()}
        raw_encounters = [
            row
            for row in _read(archive, "encounters.csv")
            if row["ORGANIZATION"] in facility_of and row["START"] >= ENCOUNTERS_SINCE
        ]
        raw_patients = {row["Id"]: row for row in _read(archive, "patients.csv")}

    facilities = [
        {
            "FACILITY_ID": fid,
            "NAME": org["NAME"],
            "ADDRESS": org["ADDRESS"],
            "CITY": org["CITY"],
            "STATE": org["STATE"],
            "ZIP": org["ZIP"],
            "PHONE": org["PHONE"],
            "SOURCE_ORGANIZATION_ID": org_id,
        }
        for fid, org_id in sorted(FACILITY_ORGANIZATIONS.items())
        for org in [organizations[org_id]]
    ]

    encounters = [
        {
            "ENCOUNTER_ID": row["Id"],
            "PATIENT_ID": row["PATIENT"],
            "FACILITY_ID": facility_of[row["ORGANIZATION"]],
            "START_TS": _timestamp(row["START"]),
            "STOP_TS": _timestamp(row["STOP"]),
            "ENCOUNTER_CLASS": row["ENCOUNTERCLASS"],
            "CODE": row["CODE"],
            "DESCRIPTION": row["DESCRIPTION"],
            "REASON_CODE": _text(row["REASONCODE"]),
            "REASON_DESCRIPTION": _text(row["REASONDESCRIPTION"]),
            "TOTAL_CLAIM_COST": Decimal(row["TOTAL_CLAIM_COST"]).quantize(Decimal("0.01")),
            "PAYER_COVERAGE": Decimal(row["PAYER_COVERAGE"]).quantize(Decimal("0.01")),
        }
        for row in sorted(raw_encounters, key=lambda r: r["Id"])
    ]

    # A patient's primary facility is where most of their encounters happened
    # (ties go to the lowest facility id).
    visits: dict[str, Counter] = defaultdict(Counter)
    for enc in encounters:
        visits[enc["PATIENT_ID"]][enc["FACILITY_ID"]] += 1
    patients = []
    for patient_id in sorted(visits):
        row = raw_patients[patient_id]
        counts = visits[patient_id]
        patients.append(
            {
                "PATIENT_ID": patient_id,
                "PRIMARY_FACILITY_ID": min(counts, key=lambda fid: (-counts[fid], fid)),
                "FIRST_NAME": row["FIRST"],
                "LAST_NAME": row["LAST"],
                "BIRTH_DATE": _date(row["BIRTHDATE"]),
                "DEATH_DATE": _date(row["DEATHDATE"]),
                "SSN": row["SSN"],
                "DRIVERS_LICENSE": _text(row["DRIVERS"]),
                "PASSPORT": _text(row["PASSPORT"]),
                "GENDER": row["GENDER"],
                "RACE": row["RACE"],
                "ETHNICITY": row["ETHNICITY"],
                "MARITAL_STATUS": _text(row["MARITAL"]),
                "ADDRESS": row["ADDRESS"],
                "CITY": row["CITY"],
                "COUNTY": row["COUNTY"],
                "STATE": row["STATE"],
                "ZIP": _text(row["ZIP"]),
                "EMAIL": patient_email(row["FIRST"], row["LAST"]),
            }
        )
    return Dataset(facilities=facilities, patients=patients, encounters=encounters)
