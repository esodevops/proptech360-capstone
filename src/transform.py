"""Topic 3: notebook stages for loading, snapshots, aggregation, and Parquet."""
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from src.ingest import PROJECT_ROOT


def start_spark():
    # Spark workers must use the same Python as the running Airflow task.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    postgres_driver = PROJECT_ROOT / 'postgresql-42.7.13.jar'
    if not postgres_driver.exists():
        raise FileNotFoundError(f'Driver not found: {postgres_driver}')
    return (SparkSession.builder.appName('PropTech360-Capstone')
            .config('spark.jars', str(postgres_driver))
            .config('spark.pyspark.python', sys.executable).master('local[2]').getOrCreate())

properties_schema = "property_id STRING, property_name STRING, city STRING"
units_schema = "unit_id STRING, property_id STRING, floor_area_sqm DOUBLE, active INT"
leases_schema = "lease_id STRING, unit_id STRING, start_date DATE, end_date DATE, monthly_rent_usd DOUBLE"
work_orders_schema = "work_order_id STRING, unit_id STRING, opened_date DATE, closed_date DATE, priority STRING, response_hours DOUBLE, cost_usd DOUBLE, status STRING, missing_response BOOLEAN, sla_compliant BOOLEAN"
meter_readings_schema = (
    "reading_id STRING, unit_id STRING, reading_month DATE, energy_kwh DOUBLE"
)

SCHEMAS = {'properties': properties_schema, 'units': units_schema, 'leases': leases_schema,
           'work_orders': work_orders_schema, 'meter_readings': meter_readings_schema}

def load_staging(spark):
    project_folder = PROJECT_ROOT
    # 2. READ STAGING DATASETS WITH EXPLICIT SCHEMAS
    data_dir = project_folder / "data" / "staging"

    properties_df = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(properties_schema)
        .csv(str(data_dir / "properties.csv"))
    )
    units_df = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(units_schema)
        .csv(str(data_dir / "units.csv"))
    )
    leases_df = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(leases_schema)
        .csv(str(data_dir / "leases.csv"))
    )
    work_orders_df = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(work_orders_schema)
        .csv(str(data_dir / "work_orders.csv"))
    )
    meter_readings_df = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(meter_readings_schema)
        .csv(str(data_dir / "meter_readings.csv"))
    )


    # 3. VALIDATE ID COLUMNS (MISSING & DUPLICATE CHECKS)
    tables_to_check = [
        (properties_df, "property_id", "properties"),
        (units_df, "unit_id", "units"),
        (leases_df, "lease_id", "leases"),
        (work_orders_df, "work_order_id", "work_orders"),
        (meter_readings_df, "reading_id", "meter_readings"),
    ]

    for df, id_col, table_name in tables_to_check:
        # Check nulls or empty strings
        missing_count = df.filter(
            F.col(id_col).isNull() | (F.trim(F.col(id_col)) == "")
        ).count()
        if missing_count > 0:
            raise ValueError(f"Missing ID found in table: {table_name}")

        # Check duplicates
        duplicate_count = df.groupBy(id_col).count().filter("count > 1").count()
        if duplicate_count > 0:
            raise ValueError(f"Duplicate ID found in table: {table_name}")
    return {'properties': properties_df, 'units': units_df, 'leases': leases_df,
            'work_orders': work_orders_df, 'meter_readings': meter_readings_df}

def make_calendar(spark):
    # 4. CREATE REPORTING MONTHS CALENDAR
    dates_data = [
        ("2026-01-01",),
        ("2026-02-01",),
        ("2026-03-01",),
        ("2026-04-01",),
        ("2026-05-01",),
        ("2026-06-01",),
    ]

    calendar_df = spark.createDataFrame(dates_data, ["month"])
    # month: Reporting month from the January-June 2026 calendar.
    calendar_df = calendar_df.withColumn("month", F.to_date(F.col("month")))
    calendar_df = calendar_df.withColumn("month_end", F.last_day(F.col("month")))
    return calendar_df

def lease_snapshot(tables, calendar):
    units_df = tables['units']
    leases_df = tables['leases']
    calendar_df = calendar
    # 5. BUILD UNIT-MONTH LEASE SNAPSHOT
    # Filter active units and build unit calendar matrix
    active_units_df = units_df.filter(F.col("active") == 1)
    unit_months_df = active_units_df.crossJoin(calendar_df)

    # Join leases to active unit months
    joined_leases_df = unit_months_df.join(leases_df, on="unit_id", how="left")

    # Filter for leases active on the month-end date
    active_lease_condition = (F.col("start_date") <= F.col("month_end")) & (
        F.col("end_date").isNull() | (F.col("end_date") >= F.col("month_end"))
    )
    current_leases_df = joined_leases_df.filter(active_lease_condition)

    # Validate no overlapping active leases per unit-month
    overlap_count = (
        current_leases_df.groupBy("unit_id", "month").count().filter("count > 1").count()
    )
    if overlap_count > 0:
        raise ValueError("Overlapping leases at month end; rerun Topic 2.")

    # Select active lease details
    current_leases_df = current_leases_df.select(
        "unit_id", "month", "monthly_rent_usd"
    ).withColumn("occupied", F.lit(1))

    # Join back to create the full unit-month snapshot
    snapshot_df = unit_months_df.join(
        current_leases_df, on=["unit_id", "month"], how="left"
    )
    snapshot_df = snapshot_df.fillna({"occupied": 0, "monthly_rent_usd": 0.0})
    return snapshot_df

def aggregate_kpis(tables, snapshot):
    snapshot_df = snapshot
    units_df = tables['units']
    work_orders_df = tables['work_orders']
    meter_readings_df = tables['meter_readings']
    # 6. COMPUTE METRICS SEPARATELY
    # A. Occupancy & Rent KPIs
    occupancy_kpi_df = snapshot_df.groupBy("property_id", "month").agg(
        # total_units: Count of accepted active units for each property and month.
        F.count("*").alias("total_units"),
        # occupied_units: Count of active units with a lease covering the month-end date.
        F.sum("occupied").alias("occupied_units"),
        # earned_monthly_rent_usd: Sum of monthly_rent_usd for those month-end leases; not cash collected.
        F.sum("monthly_rent_usd").alias("earned_monthly_rent_usd"),
    )

    # B. Work Order Maintenance KPIs
    units_prop_lookup = units_df.select("unit_id", "property_id")
    orders_df = work_orders_df.join(units_prop_lookup, on="unit_id", how="left")
    orders_df = orders_df.withColumn("month", F.trunc(F.col("opened_date"), "month"))

    # Define SLA response limits by priority
    sla_limit = (
        F.when(F.col("priority") == "CRITICAL", 4)
        .when(F.col("priority") == "HIGH", 12)
        .when(F.col("priority") == "MEDIUM", 48)
        .when(F.col("priority") == "LOW", 72)
    )

    is_resolved = F.col("status") == "RESOLVED"
    is_compliant = (
        is_resolved
        & (F.col("response_hours") >= 0)
        & (F.col("response_hours") <= sla_limit)
    )
    is_missing_resp = is_resolved & F.col("response_hours").isNull()
    is_open = F.col("status") == "OPEN"

    maintenance_kpi_df = orders_df.groupBy("property_id", "month").agg(
        # resolved_orders: Count of RESOLVED work orders, grouped by property and month opened.
        F.sum(F.when(is_resolved, 1).otherwise(0)).alias("resolved_orders"),
        # sla_compliant_orders: Count of resolved orders meeting response limits: CRITICAL 4h, HIGH 12h, MEDIUM 48h, LOW 72h.
        F.sum(F.when(is_compliant, 1).otherwise(0)).alias("sla_compliant_orders"),
        # missing_response_orders: Count of resolved work orders with missing response_hours.
        F.sum(F.when(is_missing_resp, 1).otherwise(0)).alias("missing_response_orders"),
        # open_orders: Count of OPEN work orders, grouped by property and month opened.
        F.sum(F.when(is_open, 1).otherwise(0)).alias("open_orders"),
    )

    maintenance_kpi_df = maintenance_kpi_df.withColumn(
        # sla_eligible_orders: Equals resolved_orders, including orders with missing response_hours.
        "sla_eligible_orders", F.col("resolved_orders")
    )
    maintenance_kpi_df = maintenance_kpi_df.withColumn(
        # sla_noncompliant_orders: resolved_orders minus sla_compliant_orders; includes missing responses.
        "sla_noncompliant_orders", F.col("resolved_orders") - F.col("sla_compliant_orders")
    )

    # C. Energy & Area Metrics
    readings_df = meter_readings_df.join(units_prop_lookup, on="unit_id", how="left")
    readings_df = readings_df.withColumn("month", F.trunc(F.col("reading_month"), "month"))

    energy_kpi_df = readings_df.groupBy("property_id", "month").agg(
        # energy_kwh: Sum of accepted meter_readings.energy_kwh for each property and month.
        F.sum("energy_kwh").alias("energy_kwh")
    )

    property_area_df = units_df.groupBy("property_id").agg(
        # floor_area_sqm: Sum of accepted units.floor_area_sqm per property, including inactive units.
        F.sum("floor_area_sqm").alias("floor_area_sqm")
    )
    return occupancy_kpi_df, maintenance_kpi_df, energy_kpi_df, property_area_df

def join_kpis(tables, calendar, occupancy, maintenance, energy, area):
    properties_df = tables['properties']
    calendar_df = calendar
    occupancy_kpi_df = occupancy
    maintenance_kpi_df = maintenance
    energy_kpi_df = energy
    property_area_df = area
    # 7. COMBINE ALL KPIS INTO FINAL PROPERTY-MONTH TABLE
    base_spine_df = properties_df.select("property_id").crossJoin(
        calendar_df.select("month")
    )

    final_kpis_df = base_spine_df.join(
        occupancy_kpi_df, on=["property_id", "month"], how="left"
    )
    final_kpis_df = final_kpis_df.join(
        maintenance_kpi_df, on=["property_id", "month"], how="left"
    )
    final_kpis_df = final_kpis_df.join(
        energy_kpi_df, on=["property_id", "month"], how="left"
    )
    final_kpis_df = final_kpis_df.join(property_area_df, on="property_id", how="left")

    # Fill missing metric values with zero
    fill_zero_cols = [
        "total_units",
        "occupied_units",
        "earned_monthly_rent_usd",
        "resolved_orders",
        "sla_eligible_orders",
        "sla_compliant_orders",
        "sla_noncompliant_orders",
        "missing_response_orders",
        "open_orders",
        "energy_kwh",
    ]
    final_kpis_df = final_kpis_df.fillna(0, subset=fill_zero_cols)

    # Compute rate columns using safe division
    final_kpis_df = final_kpis_df.withColumn(
        # occupancy_rate: occupied_units divided by total_units; null when the denominator is zero.
        "occupancy_rate",
        F.when(
            F.col("total_units") > 0, F.col("occupied_units") / F.col("total_units")
        ).otherwise(None),
    )

    final_kpis_df = final_kpis_df.withColumn(
        # sla_compliance_rate: sla_compliant_orders divided by sla_eligible_orders; null when the denominator is zero.
        "sla_compliance_rate",
        F.when(
            F.col("sla_eligible_orders") > 0,
            F.col("sla_compliant_orders") / F.col("sla_eligible_orders"),
        ).otherwise(None),
    )

    final_kpis_df = final_kpis_df.withColumn(
        # energy_intensity_kwh_sqm: energy_kwh divided by floor_area_sqm; null for zero or missing area.
        "energy_intensity_kwh_sqm",
        F.when(
            F.col("floor_area_sqm") > 0, F.col("energy_kwh") / F.col("floor_area_sqm")
        ).otherwise(None),
    )
    return final_kpis_df

def check_output(final_kpis_df, expected_rows=72):
    # 8. DATA QUALITY ASSERTIOS & DISPLAY
    if final_kpis_df.count() != expected_rows:
        raise ValueError(f"Unexpected property-month row count. Expected {expected_rows}.")

    unique_keys_count = final_kpis_df.select("property_id", "month").distinct().count()
    if unique_keys_count != expected_rows:
        raise ValueError("Duplicate property-month keys found.")

    invalid_occupancy = final_kpis_df.filter(
        (F.col("occupancy_rate") < 0) | (F.col("occupancy_rate") > 1)
    ).count()
    if invalid_occupancy > 0:
        raise ValueError("Invalid occupancy_rate found outside range [0, 1].")

    invalid_sla = final_kpis_df.filter(
        (F.col("sla_compliance_rate") < 0) | (F.col("sla_compliance_rate") > 1)
    ).count()
    if invalid_sla > 0:
        raise ValueError("Invalid sla_compliance_rate found outside range [0, 1].")


def check_ids(df, id_col):
    if df.filter(F.col(id_col).isNull() | (F.trim(F.col(id_col)) == '')).count() > 0:
        raise ValueError(f'Missing {id_col}; rerun Topic 2.')
    if df.groupBy(id_col).count().filter('count > 1').count() > 0:
        raise ValueError(f'Duplicate {id_col}; rerun Topic 2.')


def save_kpis(final_kpis_df):
    output_path = PROJECT_ROOT / 'data/curated/property_month_kpis'
    final_kpis_df.write.mode('overwrite').partitionBy('month').parquet(str(output_path))


def run_transform(spark):
    tables = load_staging(spark)
    calendar_df = make_calendar(spark)
    snapshot_df = lease_snapshot(tables, calendar_df)
    occupancy, maintenance, energy, area = aggregate_kpis(tables, snapshot_df)
    final_kpis_df = join_kpis(tables, calendar_df, occupancy, maintenance, energy, area).cache()
    check_output(final_kpis_df)
    save_kpis(final_kpis_df)
    return final_kpis_df


if __name__ == '__main__':
    spark = start_spark()
    try:
        run_transform(spark).orderBy('property_id', 'month').show(6, truncate=False)
    finally:
        spark.stop()
