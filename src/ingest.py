"""Load the five raw CSV files without changing them."""

import os
import hashlib
import json
from datetime import datetime, timezone
import pandas as pd

RAW_DIR = os.path.join("data", "raw")
MANIFEST_PATH = os.path.join("data", "curated", "ingestion_manifest.json")

EXPECTED_COLUMNS = {
    "properties.csv": ["property_id", "property_name", "city"],
    "units.csv": ["unit_id", "property_id", "floor_area_sqm", "active"],
    "leases.csv": ["lease_id", "unit_id", "start_date", "end_date", "monthly_rent_usd"],
    "work_orders.csv": [
        "work_order_id",
        "unit_id",
        "opened_date",
        "closed_date",
        "priority",
        "response_hours",
        "cost_usd",
        "status",
    ],
    "meter_readings.csv": ["reading_id", "unit_id", "reading_month", "energy_kwh"],
}


def ingest_source(filepath, expected_columns):
    """Return a DataFrame and audit entry, or raise a descriptive exception."""
    # Check if file exists
    if not os.path.isfile(filepath):
        raise FileNotFoundError("Source file not found: " + str(filepath))

    filename = os.path.basename(filepath)

    # Read bytes directly for hashing
    with open(filepath, "rb") as f:
        source_bytes = f.read()

    # Load CSV using file path directly
    try:
        dataset = pd.read_csv(
            filepath,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8-sig",
        )
    except Exception as error:
        raise ValueError(filename + ": empty file or invalid CSV.") from error

    # Validate columns
    missing = [col for col in expected_columns if col not in dataset.columns]
    if len(missing) > 0:
        raise ValueError(
            filename + ": missing required columns: " + ", ".join(sorted(missing))
        )

    # Validate row count
    if len(dataset) == 0:
        raise ValueError(filename + ": no data rows found.")

    manifest_entry = {
        "filename": filename,
        "row_count": len(dataset),
        "ingest_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "sha256": hashlib.sha256(source_bytes).hexdigest(),
    }
    return dataset, manifest_entry


def ingest_all(raw_dir=RAW_DIR, manifest_path=MANIFEST_PATH):
    """Load all sources and replace the manifest after validation succeeds."""
    # Prevent manifest from being saved inside the raw directory
    raw_abs = os.path.abspath(raw_dir)
    manifest_abs = os.path.abspath(manifest_path)
    if manifest_abs.startswith(raw_abs):
        raise ValueError("The manifest must be saved outside the raw source directory.")

    datasets = {}
    ingestion_manifest = []

    # Ingest each CSV file
    for filename, columns in EXPECTED_COLUMNS.items():
        file_path = os.path.join(raw_dir, filename)
        dataset, entry = ingest_source(file_path, columns)
        datasets[filename] = dataset
        ingestion_manifest.append(entry)

    # Ensure curated folder exists
    manifest_dir = os.path.dirname(manifest_path)
    if manifest_dir and not os.path.exists(manifest_dir):
        os.makedirs(manifest_dir)

    # Save manifest output
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(ingestion_manifest, f, indent=2)
        f.write("\n")

    return datasets, ingestion_manifest


if __name__ == "__main__":
    try:
        datasets, ingestion_manifest = ingest_all()
    except Exception as error:
        raise SystemExit("Ingestion failed: " + str(error)) from error

    for entry in ingestion_manifest:
        print(entry["filename"] + ": " + str(entry["row_count"]) + " rows")
    print("Manifest saved to " + MANIFEST_PATH)
