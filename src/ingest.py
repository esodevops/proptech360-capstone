"""Topic 1: read CSV files, validate them, and save an audit manifest."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / 'data/raw'
CURATED_DIR = PROJECT_ROOT / 'data/curated'
MANIFEST_PATH = CURATED_DIR / 'ingestion_manifest.json'

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
    filepath = Path(filepath)
    filename = filepath.name
    cols = expected_columns
    if not filepath.exists():
        raise FileNotFoundError(f"Source file not found: {filepath}")

    try:
        df = pd.read_csv(
            filepath, dtype=str, keep_default_na=False, encoding="utf-8-sig"
        )
    except Exception as e:
        raise ValueError(f"{filename}: empty file or invalid CSV.") from e

    missing = set(cols) - set(df.columns)
    if missing:
        raise ValueError(
            f"{filename}: missing required columns: {', '.join(sorted(missing))}"
        )
    if df.empty:
        raise ValueError(f"{filename}: no data rows found.")

    manifest_entry = {
        'filename': filename, 'row_count': len(df),
        'ingest_timestamp_utc': datetime.now(timezone.utc).isoformat(),
        'sha256': hashlib.sha256(filepath.read_bytes()).hexdigest(),
    }
    return df, manifest_entry

def ingest_all(raw_dir=RAW_DIR, manifest_path=MANIFEST_PATH):
    raw_dir = Path(raw_dir)
    manifest_path = Path(manifest_path)
    if manifest_path.resolve().is_relative_to(raw_dir.resolve()):
        raise ValueError('The manifest must be saved outside the raw source directory.')
    datasets = {}
    ingestion_manifest = []
    for filename, cols in EXPECTED_COLUMNS.items():
        df, entry = ingest_source(raw_dir / filename, cols)
        datasets[filename] = df
        ingestion_manifest.append(entry)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open('w', encoding='utf-8') as file:
        json.dump(ingestion_manifest, file, indent=2)
        file.write('\n')
    return datasets, ingestion_manifest


if __name__ == '__main__':
    datasets, ingestion_manifest = ingest_all()
    for name, df in datasets.items():
        print(f'{name}: {len(df)}')
