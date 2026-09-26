"""Check the data, keep good rows, and explain rejected rows."""

import json
from datetime import datetime, timezone
import pandas as pd
from src.ingest import PROJECT_ROOT, ingest_all

IDS = {
    "properties.csv": "property_id",
    "units.csv": "unit_id",
    "leases.csv": "lease_id",
    "work_orders.csv": "work_order_id",
    "meter_readings.csv": "reading_id",
}
SLA_HOURS = {"CRITICAL": 4, "HIGH": 12, "MEDIUM": 48, "LOW": 72}


def clean_dataframe(df):
    """Strip string whitespace and convert date/number columns simple-way."""
    for col in df.columns:
        df[col] = df[col].fillna("").astype(str).str.strip()

    # Convert known dates
    for col in [
        "start_date",
        "end_date",
        "opened_date",
        "closed_date",
        "reading_month",
    ]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors="coerce")

    # Convert known numbers
    for col in [
        "floor_area_sqm",
        "monthly_rent_usd",
        "cost_usd",
        "energy_kwh",
        "response_hours",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def profile_table(df, identifier):
    """Count missing values and duplicate IDs before cleaning."""
    missing = df.fillna("").astype(str).apply(lambda column: column.str.strip() == "")
    return {
        "rows": len(df), "nulls": missing.sum().to_dict(),
        "duplicate_identifiers": int(df.duplicated(identifier).sum()),
        "types": df.dtypes.astype(str).to_dict(),
    }


def check_row_errors(row, name, clean_datasets):
    """Simple list of checks for each row."""
    errors = []

    # 1. Check Primary ID
    id_col = IDS[name]
    if pd.isna(row.get(id_col)) or str(row.get(id_col)).strip() == "":
        errors.append("MISSING_ID")

    # Required dates and numbers cannot be missing or invalid.
    for col in ["start_date", "opened_date", "reading_month"]:
        if col in row and pd.isna(row[col]):
            errors.append("INVALID_" + col.upper())
    for col in ["floor_area_sqm", "monthly_rent_usd", "cost_usd", "energy_kwh", "response_hours"]:
        if col in row:
            if col == "response_hours" and pd.isna(row[col]):
                continue  # A blank response is retained and flagged below.
            if pd.isna(row[col]) or row[col] < 0 or row[col] == float("inf"):
                errors.append("INVALID_" + col.upper())

    # 2. Foreign Key checks
    if name == "units.csv":
        valid_props = clean_datasets["properties.csv"]["property_id"]
        if row["property_id"] not in valid_props.values:
            errors.append("UNKNOWN_PROPERTY")
    elif name != "properties.csv" and "unit_id" in row:
        valid_units = clean_datasets["units.csv"]["unit_id"]
        if row["unit_id"] not in valid_units.values:
            errors.append("UNKNOWN_UNIT")

    # 3. File-specific checks
    if name == "properties.csv":
        if not row["property_name"]:
            errors.append("MISSING_PROPERTY_NAME")
        if not row["city"]:
            errors.append("MISSING_CITY")

    elif name == "units.csv":
        if row["floor_area_sqm"] == 0:
            errors.append("ZERO_AREA")
        if row["active"] not in ["0", "1"]:
            errors.append("INVALID_ACTIVE")

    elif name == "leases.csv":
        if row["start_date"] > row["end_date"]:
            errors.append("LEASE_DATE_ORDER")

    elif name == "work_orders.csv":
        if row["priority"] not in SLA_HOURS:
            errors.append("INVALID_PRIORITY")
        if row["status"] not in ["OPEN", "RESOLVED"]:
            errors.append("INVALID_STATUS")
        if row["status"] == "RESOLVED" and pd.isna(row["closed_date"]):
            errors.append("MISSING_CLOSED_DATE")
        if row["status"] == "OPEN" and pd.notna(row["closed_date"]):
            errors.append("OPEN_WITH_CLOSED_DATE")
        if row["opened_date"] > row["closed_date"]:
            errors.append("WORK_ORDER_DATE_ORDER")

    elif name == "meter_readings.csv":
        start, end = pd.Timestamp("2026-01-01"), pd.Timestamp("2026-06-30")
        if not (start <= row["reading_month"] <= end):
            errors.append("READING_OUT_OF_RANGE")

    return errors


def validate_and_quarantine(datasets, run_id):
    clean, quarantine, summary = {}, {}, {}

    for name, id_col in IDS.items():
        original = datasets[name].copy().reset_index(drop=True)
        df = clean_dataframe(original.copy())

        # Track problems per row index
        problems = {i: [] for i in range(len(df))}

        # Keep the first exact copy, but reject conflicting rows with the same ID.
        exact = original.duplicated()
        duplicates = df.loc[~exact].duplicated(id_col, keep=False)
        for i in df.index:
            if exact[i]:
                problems[i].append("EXACT_DUPLICATE")
            if duplicates.get(i, False):
                problems[i].append("DUPLICATE_ID")

        for i, row in df.iterrows():
            problems[i].extend(check_row_errors(row, name, clean))
            for col in ["end_date", "closed_date", "response_hours"]:
                if col in df:
                    value = original.loc[i, col]
                    if pd.notna(value) and str(value).strip() and pd.isna(row[col]):
                        problems[i].append("INVALID_" + col.upper())

        if name == "meter_readings.csv":
            readings = df.loc[~exact].copy()
            readings["reading_month"] = readings["reading_month"].dt.strftime("%Y-%m")
            repeated = readings.duplicated(["unit_id", "reading_month"], keep=False)
            for i in readings.index[repeated]:
                problems[i].append("DUPLICATE_UNIT_MONTH")

        if name == "leases.csv":
            valid = df.loc[[i for i in df.index if not problems[i]]].copy()
            # A blank end date means that the lease is still active.
            valid["end_date"] = valid["end_date"].fillna(pd.Timestamp.max)
            for _, leases in valid.groupby("unit_id"):
                for i, a in leases.iterrows():
                    for j, b in leases.iterrows():
                        if i < j and a["start_date"] <= b["end_date"] and b["start_date"] <= a["end_date"]:
                            problems[i].append("OVERLAPPING_LEASE")
                            problems[j].append("OVERLAPPING_LEASE")

        if name == "work_orders.csv":
            df["missing_response"] = df["response_hours"].isna()
            limit = df["priority"].map(SLA_HOURS)
            df["sla_compliant"] = ((df["status"] == "RESOLVED") & (df["response_hours"] <= limit)).fillna(False)

        # Separate clean vs rejected
        rejected_mask = pd.Series([bool(problems[i]) for i in df.index], dtype=bool)

        clean_df = df[~rejected_mask].copy()
        quarantine_df = original[rejected_mask].copy()

        # Add tracking metadata to quarantine output
        reasons = [
            ";".join(sorted(set(problems[i]))) for i in range(len(df)) if rejected_mask[i]
        ]
        quarantine_df["reason_code"] = reasons
        quarantine_df["run_id"] = run_id

        # Save to result sets
        clean[name] = clean_df
        quarantine[name] = quarantine_df

        # Save simple count metrics
        rule_counts = {}
        for errors in problems.values():
            for reason in set(errors):
                rule_counts[reason] = rule_counts.get(reason, 0) + 1
        if name == "work_orders.csv":
            rule_counts["MISSING_RESPONSE_RETAINED"] = int(clean_df["missing_response"].sum())
        summary[name] = {
            **profile_table(original, id_col),
            "exact_duplicates": int(exact.sum()),
            "rule_counts": rule_counts,
            "run_id": run_id,
            "input": len(df),
            "accepted": len(clean_df),
            "quarantined": len(quarantine_df),
        }

    return clean, quarantine, summary


def run_quality():
    datasets, _ = ingest_all()
    run_id = datetime.now(timezone.utc).isoformat()
    clean, quarantine, summary = validate_and_quarantine(datasets, run_id)

    # Save outputs
    for folder, tables in [("staging", clean), ("quarantine", quarantine)]:
        output_dir = PROJECT_ROOT / "data" / folder
        output_dir.mkdir(parents=True, exist_ok=True)
        for name, table in tables.items():
            table.to_csv(output_dir / name, index=False)

    report_file = PROJECT_ROOT / "data/curated/quality_summary.json"
    report_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return clean, quarantine, summary


if __name__ == "__main__":
    clean, quarantine, summary = run_quality()
    print(pd.DataFrame(summary).T[["input", "accepted", "quarantined"]])
