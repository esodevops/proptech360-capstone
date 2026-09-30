"""Topic 2: the same profiling and row checks used in the notebook."""
import json
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
from src.ingest import PROJECT_ROOT, ingest_all

def profile_table(df: pd.DataFrame, identifier: str) -> dict:
    nulls = {}
    for col in df.columns:
        # Check for NaN, empty strings, or stringified whitespace
        empty_count = 0
        for val in df[col]:
            if pd.isna(val) or str(val).strip() == "":
                empty_count += 1
        nulls[col] = empty_count

    return {
        "rows": len(df),
        "nulls": nulls,
        "duplicate_identifiers": int(df.duplicated(identifier).sum()),
        "types": df.dtypes.astype(str).to_dict(),
    }

def validate_and_quarantine(datasets: dict, run_id: str):
    id_columns = {
        "properties.csv": "property_id",
        "units.csv": "unit_id",
        "leases.csv": "lease_id",
        "work_orders.csv": "work_order_id",
        "meter_readings.csv": "reading_id",
    }
    sla_hours = {"CRITICAL": 4, "HIGH": 12, "MEDIUM": 48, "LOW": 72}
    clean, quarantine, summary = {}, {}, {}

    for name, id_col in id_columns.items():
        original = datasets[name].copy().reset_index(drop=True)
        df = original.copy()

        # Clean string whitespace
        for col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

        # Convert known dates
        date_cols = [
            "start_date",
            "end_date",
            "opened_date",
            "closed_date",
            "reading_month",
        ]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors="coerce")

        # Convert known numbers
        num_cols = [
            "floor_area_sqm",
            "monthly_rent_usd",
            "cost_usd",
            "energy_kwh",
            "response_hours",
        ]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        problems = {i: [] for i in range(len(df))}

        # Duplicate ID and Exact Duplicate checks
        exact = original.duplicated()
        duplicates = df.loc[~exact].duplicated(id_col, keep=False)
        for i in df.index:
            if exact[i]:
                problems[i].append("EXACT_DUPLICATE")
            if duplicates.get(i, False):
                problems[i].append("DUPLICATE_ID")

        # Row-by-row validation checks
        for i, row in df.iterrows():
            errors = []

            # 1. Primary ID
            if pd.isna(row.get(id_col)) or str(row.get(id_col)).strip() == "":
                errors.append("MISSING_ID")

            # Mandatory dates
            for col in ["start_date", "opened_date", "reading_month"]:
                if col in row and pd.isna(row[col]):
                    errors.append("INVALID_" + col.upper())

            # Mandatory / Valid numbers
            for col in num_cols:
                if col in row:
                    if col == "response_hours" and pd.isna(row[col]):
                        continue
                    if pd.isna(row[col]) or row[col] < 0 or row[col] == float("inf"):
                        errors.append("INVALID_" + col.upper())

            # 2. Foreign Key checks
            if name == "units.csv":
                valid_props = list(clean["properties.csv"]["property_id"])
                if row["property_id"] not in valid_props:
                    errors.append("UNKNOWN_PROPERTY")
            elif name != "properties.csv" and "unit_id" in row:
                valid_units = list(clean["units.csv"]["unit_id"])
                if row["unit_id"] not in valid_units:
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
                if str(row["active"]) not in ["0", "1"]:
                    errors.append("INVALID_ACTIVE")

            elif name == "leases.csv":
                if pd.notna(row["start_date"]) and pd.notna(row["end_date"]):
                    if row["start_date"] > row["end_date"]:
                        errors.append("LEASE_DATE_ORDER")

            elif name == "work_orders.csv":
                if row["priority"] not in sla_hours:
                    errors.append("INVALID_PRIORITY")
                if row["status"] not in ["OPEN", "RESOLVED"]:
                    errors.append("INVALID_STATUS")
                if row["status"] == "RESOLVED" and pd.isna(row["closed_date"]):
                    errors.append("MISSING_CLOSED_DATE")
                if row["status"] == "OPEN" and pd.notna(row["closed_date"]):
                    errors.append("OPEN_WITH_CLOSED_DATE")
                if pd.notna(row["opened_date"]) and pd.notna(row["closed_date"]):
                    if row["opened_date"] > row["closed_date"]:
                        errors.append("WORK_ORDER_DATE_ORDER")

            elif name == "meter_readings.csv":
                start, end = pd.Timestamp("2026-01-01"), pd.Timestamp("2026-06-30")
                if pd.isna(row["reading_month"]) or not (
                    start <= row["reading_month"] <= end
                ):
                    errors.append("READING_OUT_OF_RANGE")

            problems[i].extend(errors)

            # Re-check lost inputs from non-required fields
            for col in ["end_date", "closed_date", "response_hours"]:
                if col in df:
                    value = original.loc[i, col]
                    if (
                        pd.notna(value)
                        and str(value).strip() != ""
                        and pd.isna(row[col])
                    ):
                        problems[i].append("INVALID_" + col.upper())

        # Additional multi-row validations
        if name == "meter_readings.csv":
            readings = df.loc[~exact].copy()
            readings["reading_month"] = readings["reading_month"].dt.strftime("%Y-%m")
            repeated = readings.duplicated(["unit_id", "reading_month"], keep=False)
            for i in readings.index[repeated]:
                problems[i].append("DUPLICATE_UNIT_MONTH")

        if name == "leases.csv":
            valid_indices = [i for i in df.index if len(problems[i]) == 0]
            valid = df.loc[valid_indices].copy()
            valid["end_date"] = valid["end_date"].fillna(pd.Timestamp.max)
            for _, leases in valid.groupby("unit_id"):
                for i, a in leases.iterrows():
                    for j, b in leases.iterrows():
                        if (
                            i < j
                            and a["start_date"] <= b["end_date"]
                            and b["start_date"] <= a["end_date"]
                        ):
                            problems[i].append("OVERLAPPING_LEASE")
                            problems[j].append("OVERLAPPING_LEASE")

        if name == "work_orders.csv":
            df["missing_response"] = df["response_hours"].isna()
            limit = df["priority"].map(sla_hours)
            df["sla_compliant"] = (
                (df["status"] == "RESOLVED") & (df["response_hours"] <= limit)
            ).fillna(False)

        # Separate clean vs rejected
        rejected_mask = [len(problems[i]) > 0 for i in range(len(df))]

        clean_df = df[[not r for r in rejected_mask]].copy()
        quarantine_df = original[rejected_mask].copy()

        # Add tracking metadata
        reasons = [
            ";".join(sorted(set(problems[i])))
            for i in range(len(df))
            if rejected_mask[i]
        ]
        quarantine_df["reason_code"] = reasons
        quarantine_df["run_id"] = run_id

        clean[name] = clean_df
        quarantine[name] = quarantine_df

        # Aggregate summary metrics
        rule_counts = {}
        for errors in problems.values():
            for reason in set(errors):
                rule_counts[reason] = rule_counts.get(reason, 0) + 1

        if name == "work_orders.csv":
            rule_counts["MISSING_RESPONSE_RETAINED"] = int(
                clean_df["missing_response"].sum()
            )

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
    datasets, ingestion_manifest = ingest_all()
    run_id = datetime.now(timezone.utc).isoformat()
    clean_datasets, quarantine_datasets, quality_summary = validate_and_quarantine(
        datasets, run_id
    )

    for folder, tables in [
        ("staging", clean_datasets),
        ("quarantine", quarantine_datasets),
    ]:
        output = PROJECT_ROOT / "data" / folder
        output.mkdir(parents=True, exist_ok=True)
        for filename, table in tables.items():
            table.to_csv(output / filename, index=False)

    for counts in quality_summary.values():
        assert counts["input"] == counts["accepted"] + counts["quarantined"]

    report = PROJECT_ROOT / "data/curated/quality_summary.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(quality_summary, indent=2), encoding="utf-8")
    return clean_datasets, quarantine_datasets, quality_summary

if __name__ == '__main__':
    clean_datasets, quarantine_datasets, quality_summary = run_quality()
    print(pd.DataFrame(quality_summary).T[['input', 'accepted', 'quarantined']])
