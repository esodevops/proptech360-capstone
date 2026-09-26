"""Load the five raw CSV files without changing them."""

import hashlib
from io import BytesIO
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
MANIFEST_PATH = PROJECT_ROOT / "data" / "curated" / "ingestion_manifest.json"

EXPECTED_COLUMNS = {
    "properties.csv": {"property_id", "property_name", "city"},
    "units.csv": {"unit_id", "property_id", "floor_area_sqm", "active"},
    "leases.csv": {"lease_id", "unit_id", "start_date", "end_date", "monthly_rent_usd"},
    "work_orders.csv": {
        "work_order_id",
        "unit_id",
        "opened_date",
        "closed_date",
        "priority",
        "response_hours",
        "cost_usd",
        "status",
    },
    "meter_readings.csv": {"reading_id", "unit_id", "reading_month", "energy_kwh"},
}


def ingest_source(path, expected_columns):
    """Return a DataFrame and audit entry, or raise a descriptive exception."""
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"Source file not found: {path}")

    # Use the same bytes for loading and hashing.
    source_bytes = path.read_bytes()
    try:
        dataset = pd.read_csv(
            BytesIO(source_bytes),
            dtype="string",
            keep_default_na=False,
            encoding="utf-8-sig",
        )
    except (
        pd.errors.EmptyDataError,
        pd.errors.ParserError,
        UnicodeDecodeError,
    ) as error:
        raise ValueError(f"{path.name}: empty file or invalid CSV.") from error

    missing = set(expected_columns) - set(dataset.columns)
    if missing:
        raise ValueError(
            f'{path.name}: missing required columns: {", ".join(sorted(missing))}'
        )
    if dataset.empty:
        raise ValueError(f"{path.name}: no data rows found.")

    manifest_entry = {
        "filename": path.name,
        "row_count": len(dataset),
        "ingest_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "sha256": hashlib.sha256(source_bytes).hexdigest(),
    }
    return dataset, manifest_entry


def ingest_all(raw_dir=RAW_DIR, manifest_path=MANIFEST_PATH):
    """Load all sources and replace the manifest after validation succeeds."""
    raw_dir = Path(raw_dir)
    manifest_path = Path(manifest_path)
    # Prevent an accidental output path from overwriting any raw source.
    if manifest_path.resolve().is_relative_to(raw_dir.resolve()):
        raise ValueError("The manifest must be saved outside the raw source directory.")

    datasets = {}
    ingestion_manifest = []
    for filename, columns in EXPECTED_COLUMNS.items():
        dataset, entry = ingest_source(raw_dir / filename, columns)
        datasets[filename] = dataset
        ingestion_manifest.append(entry)

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(ingestion_manifest, indent=2) + "\n", encoding="utf-8"
    )
    return datasets, ingestion_manifest


if __name__ == "__main__":
    try:
        datasets, ingestion_manifest = ingest_all()
    except (OSError, ValueError) as error:
        raise SystemExit(f"Ingestion failed: {error}") from error
    for entry in ingestion_manifest:
        print(f'{entry["filename"]}: {entry["row_count"]:,} rows')
    print(f"Manifest saved to {MANIFEST_PATH}")
