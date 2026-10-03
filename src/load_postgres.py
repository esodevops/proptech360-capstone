"""Topic 4: notebook SQL, psycopg2 batch loading, and Spark JDBC validation."""
import os
import psycopg2
import psycopg2.extras
from psycopg2 import sql
import pandas as pd
from dotenv import dotenv_values
from src.ingest import PROJECT_ROOT
from src.transform import start_spark

# 1. CONFIGURATION & ENVIRONMENT

def database_settings():
    settings = dotenv_values(PROJECT_ROOT / '.env', interpolate=False)
    settings.update(os.environ)
    return settings


def database_configured():
    settings = database_settings()
    return bool(settings.get('DATABASE_URL') or (settings.get('DB_NAME') and settings.get('DB_USER')))


def connect(dbname=None):
    settings = database_settings()
    if settings.get('DATABASE_URL'):
        url = settings['DATABASE_URL'].replace('postgresql+psycopg://', 'postgresql://', 1)
        if dbname:
            return psycopg2.connect(url, dbname=dbname, connect_timeout=5)
        return psycopg2.connect(url, connect_timeout=5)
    if not settings.get('DB_NAME') or not settings.get('DB_USER'):
        raise ValueError('Set DB_NAME and DB_USER in .env, or provide DATABASE_URL.')
    return psycopg2.connect(host=settings.get('DB_HOST') or 'localhost',
        port=settings.get('DB_PORT') or '5432', dbname=dbname or settings['DB_NAME'],
        user=settings['DB_USER'], password=settings.get('DB_PASSWORD'), connect_timeout=5)


def create_database():
    """Create the configured database once; leave an existing database unchanged."""
    settings = database_settings()
    dbname = settings.get('DB_NAME')
    if settings.get('DATABASE_URL'):
        url = settings['DATABASE_URL'].replace('postgresql+psycopg://', 'postgresql://', 1)
        dbname = psycopg2.extensions.parse_dsn(url).get('dbname')
    if not dbname:
        raise ValueError('Set DB_NAME or include a database name in DATABASE_URL.')

    # Connect to the existing maintenance database before creating our database.
    connection = connect(dbname='postgres')
    try:
        connection.autocommit = True  # CREATE DATABASE cannot run in a transaction.
        with connection.cursor() as cursor:
            cursor.execute('SELECT 1 FROM pg_database WHERE datname = %s', (dbname,))
            if cursor.fetchone() is None:
                cursor.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(dbname)))
    finally:
        connection.close()


def jdbc_settings():
    settings = database_settings()
    if settings.get('DATABASE_URL'):
        url = settings['DATABASE_URL'].replace('postgresql+psycopg://', 'postgresql://', 1)
        values = psycopg2.extensions.parse_dsn(url)
    else:
        values = {'host': settings.get('DB_HOST') or 'localhost',
                  'port': settings.get('DB_PORT') or '5432',
                  'dbname': settings.get('DB_NAME'), 'user': settings.get('DB_USER'),
                  'password': settings.get('DB_PASSWORD')}
    jdbc_url = f"jdbc:postgresql://{values.get('host', 'localhost')}:{values.get('port', '5432')}/{values['dbname']}"
    return jdbc_url, {'user': values['user'], 'password': values.get('password') or '',
                      'driver': 'org.postgresql.Driver'}

# 2. SQL SCHEMAS AND QUERIES
schema_sql = """
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.dim_property (
    property_id TEXT PRIMARY KEY CHECK (property_id != ''),
    property_name TEXT NOT NULL CHECK (property_name != ''),
    city TEXT NOT NULL CHECK (city != '')
);

CREATE TABLE IF NOT EXISTS mart.dim_month (
    month DATE PRIMARY KEY CHECK (EXTRACT(DAY FROM month) = 1)
);

CREATE TABLE IF NOT EXISTS mart.fact_property_month (
    property_id TEXT REFERENCES mart.dim_property(property_id) NOT NULL,
    month DATE REFERENCES mart.dim_month(month) NOT NULL,
    total_units INTEGER NOT NULL CHECK (total_units >= 0),
    occupied_units INTEGER NOT NULL CHECK (occupied_units BETWEEN 0 AND total_units),
    earned_monthly_rent_usd NUMERIC(18,2) NOT NULL CHECK (earned_monthly_rent_usd >= 0),
    resolved_orders INTEGER NOT NULL CHECK (resolved_orders >= 0),
    sla_eligible_orders INTEGER NOT NULL CHECK (sla_eligible_orders = resolved_orders),
    sla_compliant_orders INTEGER NOT NULL CHECK (sla_compliant_orders BETWEEN 0 AND sla_eligible_orders),
    sla_noncompliant_orders INTEGER NOT NULL CHECK (sla_noncompliant_orders = resolved_orders - sla_compliant_orders),
    missing_response_orders INTEGER NOT NULL CHECK (missing_response_orders BETWEEN 0 AND sla_noncompliant_orders),
    open_orders INTEGER NOT NULL CHECK (open_orders >= 0),
    energy_kwh NUMERIC(18,2) NOT NULL CHECK (energy_kwh >= 0),
    floor_area_sqm NUMERIC(18,2) CHECK (floor_area_sqm >= 0),
    occupancy_rate NUMERIC GENERATED ALWAYS AS 
        (CAST(occupied_units AS NUMERIC) / NULLIF(total_units, 0)) STORED,
    sla_compliance_rate NUMERIC GENERATED ALWAYS AS 
        (CAST(sla_compliant_orders AS NUMERIC) / NULLIF(sla_eligible_orders, 0)) STORED,
    energy_intensity_kwh_sqm NUMERIC GENERATED ALWAYS AS 
        (energy_kwh / NULLIF(floor_area_sqm, 0)) STORED,
    PRIMARY KEY (property_id, month)
);

CREATE INDEX IF NOT EXISTS fact_month_idx ON mart.fact_property_month(month);
CREATE INDEX IF NOT EXISTS property_city_idx ON mart.dim_property(city);

CREATE OR REPLACE VIEW mart.portfolio_dashboard AS
SELECT month,
       SUM(total_units) AS total_units,
       SUM(occupied_units) AS occupied_units,
       CAST(SUM(occupied_units) AS NUMERIC) / NULLIF(SUM(total_units), 0) AS occupancy_rate,
       SUM(earned_monthly_rent_usd) AS earned_monthly_rent_usd,
       SUM(sla_eligible_orders) AS sla_eligible_orders,
       SUM(sla_compliant_orders) AS sla_compliant_orders,
       SUM(open_orders) AS open_orders,
       CAST(SUM(sla_compliant_orders) AS NUMERIC) / NULLIF(SUM(sla_eligible_orders), 0) AS sla_compliance_rate,
       SUM(energy_kwh) AS energy_kwh,
       SUM(floor_area_sqm) AS floor_area_sqm,
       SUM(energy_kwh) / NULLIF(SUM(floor_area_sqm), 0) AS energy_intensity_kwh_sqm
FROM mart.fact_property_month
GROUP BY month;
"""

property_insert = """
INSERT INTO mart.dim_property (property_id, property_name, city) VALUES (%s, %s, %s)
ON CONFLICT (property_id) DO UPDATE SET
    property_name = EXCLUDED.property_name, city = EXCLUDED.city
"""

month_insert = """
INSERT INTO mart.dim_month (month) VALUES (%s) ON CONFLICT (month) DO NOTHING
"""

fact_columns = [
    'property_id', 'month', 'total_units', 'occupied_units', 'earned_monthly_rent_usd',
    'resolved_orders', 'sla_eligible_orders', 'sla_compliant_orders', 'sla_noncompliant_orders',
    'missing_response_orders', 'open_orders', 'energy_kwh', 'floor_area_sqm',
]

stage_sql = """
CREATE TEMP TABLE stage_property_month ON COMMIT DROP AS
SELECT property_id, month, total_units, occupied_units, earned_monthly_rent_usd,
       resolved_orders, sla_eligible_orders, sla_compliant_orders, sla_noncompliant_orders,
       missing_response_orders, open_orders, energy_kwh, floor_area_sqm
FROM mart.fact_property_month WITH NO DATA
"""

stage_insert = """INSERT INTO stage_property_month VALUES 
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

upsert_sql = """
INSERT INTO mart.fact_property_month (
    property_id, month, total_units, occupied_units, earned_monthly_rent_usd,
    resolved_orders, sla_eligible_orders, sla_compliant_orders, sla_noncompliant_orders,
    missing_response_orders, open_orders, energy_kwh, floor_area_sqm
)
SELECT property_id, month, total_units, occupied_units, earned_monthly_rent_usd,
       resolved_orders, sla_eligible_orders, sla_compliant_orders, sla_noncompliant_orders,
       missing_response_orders, open_orders, energy_kwh, floor_area_sqm
FROM stage_property_month
ON CONFLICT (property_id, month) DO UPDATE SET
    total_units = EXCLUDED.total_units,
    occupied_units = EXCLUDED.occupied_units,
    earned_monthly_rent_usd = EXCLUDED.earned_monthly_rent_usd,
    resolved_orders = EXCLUDED.resolved_orders,
    sla_eligible_orders = EXCLUDED.sla_eligible_orders,
    sla_compliant_orders = EXCLUDED.sla_compliant_orders,
    sla_noncompliant_orders = EXCLUDED.sla_noncompliant_orders,
    missing_response_orders = EXCLUDED.missing_response_orders,
    open_orders = EXCLUDED.open_orders,
    energy_kwh = EXCLUDED.energy_kwh,
    floor_area_sqm = EXCLUDED.floor_area_sqm;
"""

constraint_sql = """
SELECT table_name, constraint_name, constraint_type
FROM information_schema.table_constraints WHERE table_schema = 'mart'
"""

queries = [
    """
    SELECT p.property_id, p.property_name,
           DENSE_RANK() OVER (ORDER BY AVG(f.occupancy_rate) DESC NULLS LAST) AS occupancy_rank,
           AVG(f.occupancy_rate) AS average_monthly_occupancy,
           SUM(f.occupied_units) AS occupied_unit_months,
           SUM(f.total_units) AS available_unit_months,
           COUNT(f.occupancy_rate) AS months_with_defined_rate
    FROM mart.fact_property_month f JOIN mart.dim_property p USING (property_id)
    WHERE month BETWEEN DATE '2026-01-01' AND DATE '2026-06-01'
    GROUP BY p.property_id, p.property_name
    ORDER BY occupancy_rank, p.property_id
    """,
    """
    SELECT f.month, p.city, SUM(f.earned_monthly_rent_usd) AS earned_monthly_rent_usd
    FROM mart.fact_property_month f JOIN mart.dim_property p USING (property_id)
    WHERE month BETWEEN DATE '2026-01-01' AND DATE '2026-06-01'
    GROUP BY f.month, p.city
    ORDER BY f.month, p.city
    """,
    """
    SELECT property_id, SUM(resolved_orders) AS resolved_orders,
           SUM(sla_eligible_orders) AS eligible_resolved_orders,
           SUM(sla_compliant_orders) AS compliant_orders,
           SUM(sla_noncompliant_orders) AS noncompliant_orders,
           SUM(open_orders) AS excluded_unresolved_orders,
           SUM(missing_response_orders) AS included_missing_response_orders,
           0 AS excluded_missing_response_orders,
           CAST(SUM(sla_compliant_orders) AS NUMERIC) / NULLIF(SUM(sla_eligible_orders), 0) AS sla_compliance_rate
    FROM mart.fact_property_month
    WHERE month BETWEEN DATE '2026-01-01' AND DATE '2026-06-01'
    GROUP BY property_id
    ORDER BY property_id
    """,
    """
    SELECT f.property_id, f.month, f.energy_kwh, f.energy_intensity_kwh_sqm,
           p.energy_intensity_kwh_sqm AS portfolio_energy_intensity
    FROM mart.fact_property_month f JOIN mart.portfolio_dashboard p USING (month)
    WHERE f.energy_intensity_kwh_sqm > p.energy_intensity_kwh_sqm
      AND f.month BETWEEN DATE '2026-01-01' AND DATE '2026-06-01'
    ORDER BY f.month, f.property_id
    """,
    """
    SELECT * FROM mart.portfolio_dashboard
    WHERE month BETWEEN DATE '2026-01-01' AND DATE '2026-06-01'
    ORDER BY month
    """
]

def load_mart(spark=None):
    if spark is None:
        spark = start_spark()
    # 3. DATA PREPARATION 
    # Read the validated file directly; another cell may have reused "tables".
    properties = spark.read.csv(
        str(PROJECT_ROOT / 'data/staging/properties.csv'),
        header=True,
        schema='property_id STRING, property_name STRING, city STRING',
        mode='FAILFAST'
    )
    facts = spark.read.parquet(str(PROJECT_ROOT / 'data/curated/property_month_kpis'))
    months = facts.select('month').distinct()

    assert facts.count() == 72
    assert facts.select('property_id', 'month').distinct().count() == 72

    # Extract PySpark DataFrame data into standard Python lists of tuples for loading
    props_data = [tuple(row) for row in properties.collect()]
    months_data = [(row['month'],) for row in months.collect()]
    facts_data = [tuple(row) for row in facts.select(*fact_columns).collect()]


    # 4. DATABASE INITIALIZATION & LOADING
    create_database()
    conn = connect()
    conn.autocommit = False 
    cursor = conn.cursor()

    try:
        cursor.execute(schema_sql)
        cursor.execute(stage_sql)
    
        # Batch execute the inserts using standard python drivers
        psycopg2.extras.execute_batch(cursor, property_insert, props_data)
        psycopg2.extras.execute_batch(cursor, month_insert, months_data)
        psycopg2.extras.execute_batch(cursor, stage_insert, facts_data)

        counts = []
        for run_number in range(2):
            cursor.execute(upsert_sql)
            cursor.execute("SELECT COUNT(*) FROM mart.fact_property_month")
            counts.append(cursor.fetchone()[0])

        assert counts == [72, 72], 'Expected 72 rows after both loads.'
        conn.commit()
        print('Both loads have 72 fact rows.')
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
    return counts[-1]

# 5. VALIDATION

def read_curated():
    data = pd.read_parquet(PROJECT_ROOT / 'data/curated/property_month_kpis')
    data['month'] = pd.to_datetime(data['month']).dt.date
    if len(data) != 72 or data.duplicated(['property_id', 'month']).any():
        raise ValueError('Expected 72 unique property-month rows.')
    return data


def query_table(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return pd.DataFrame(cursor.fetchall(), columns=[column.name for column in cursor.description])


def verify_mart(spark=None):
    if spark is None:
        spark = start_spark()
    jdbc_url, jdbc_options = jdbc_settings()
    facts = spark.read.parquet(str(PROJECT_ROOT / 'data/curated/property_month_kpis'))
    stored = spark.read.jdbc(jdbc_url, 'mart.fact_property_month', properties=jdbc_options)
    expected = facts.orderBy('property_id', 'month').toPandas()
    actual = stored.select(*facts.columns).orderBy('property_id', 'month').toPandas()
    for column in facts.columns:
        if column not in ['property_id', 'month']:
            expected[column] = pd.to_numeric(expected[column])
            actual[column] = pd.to_numeric(actual[column])
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False, check_exact=False, rtol=1e-9, atol=1e-7)
    return '72 rows reconciled with Spark Parquet.'


# 6. ANALYTICS & EXPORT

def save_query_results(spark=None):
    if spark is None:
        spark = start_spark()
    jdbc_url, jdbc_options = jdbc_settings()
    evidence_dir = PROJECT_ROOT / 'evidence' / '04_postgres'
    evidence_dir.mkdir(parents=True, exist_ok=True)
    constraints = spark.read.jdbc(jdbc_url, f'({constraint_sql}) AS checks', properties=jdbc_options)
    constraints.toPandas().to_csv(evidence_dir / 'constraints.csv', index=False)
    for idx, q_sql in enumerate(queries, start=1):
        res_df = spark.read.jdbc(jdbc_url, f'({q_sql}) AS result', properties=jdbc_options)
        res_df.show(10, truncate=False)
        res_df.toPandas().to_csv(evidence_dir / f'query_{idx}.csv', index=False)
    return evidence_dir


if __name__ == '__main__':
    spark = start_spark()
    try:
        load_mart(spark)
        print(verify_mart(spark))
        print('Evidence saved to:', save_query_results(spark))
    finally:
        spark.stop()
