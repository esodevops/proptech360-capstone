"""Load the small curated dataset into PostgreSQL without duplicate rows."""

import os
import pandas as pd
import psycopg
from dotenv import dotenv_values
from src.ingest import PROJECT_ROOT

SQL_DIR = PROJECT_ROOT / 'sql'
FACT_COLUMNS = [
    'property_id', 'month', 'total_units', 'occupied_units', 'earned_monthly_rent_usd',
    'resolved_orders', 'sla_eligible_orders', 'sla_compliant_orders', 'sla_noncompliant_orders',
    'missing_response_orders', 'open_orders', 'energy_kwh', 'floor_area_sqm',
]


def database_settings():
    """Read .env privately; terminal environment settings take priority."""
    settings = dotenv_values(PROJECT_ROOT / '.env', interpolate=False)
    settings.update(os.environ)
    return settings


def database_configured():
    settings = database_settings()
    return bool(settings.get('DATABASE_URL') or
                (settings.get('DB_NAME') and settings.get('DB_USER')))


def connect():
    """Accept a URL or separate DB_* settings without exposing credentials."""
    settings = database_settings()
    url = settings.get('DATABASE_URL')
    if url:
        connection = psycopg.connect(
            url.replace('postgresql+psycopg://', 'postgresql://', 1), connect_timeout=5)
    else:
        if not settings.get('DB_NAME') or not settings.get('DB_USER'):
            raise ValueError('Set DB_NAME and DB_USER in .env, or provide DATABASE_URL.')
        connection = psycopg.connect(
            dbname=settings['DB_NAME'], user=settings['DB_USER'],
            password=settings.get('DB_PASSWORD'),
            host=settings.get('DB_HOST') or 'localhost',
            port=settings.get('DB_PORT') or '5432', connect_timeout=5)
    return connection


def read_curated():
    """Read only the small property-month summary, not the raw fact tables."""
    data = pd.read_parquet(PROJECT_ROOT / 'data/curated/property_month_kpis')
    data['month'] = pd.to_datetime(data['month']).dt.date
    if 'missing_response_orders' not in data:
        raise ValueError('Rerun Topic 3 to include missing_response_orders in Parquet.')
    if len(data) != 72 or data.duplicated(['property_id', 'month']).any():
        raise ValueError('Expected 72 unique property-month rows from Topic 3.')
    return data


def load_mart():
    """Load dimensions, stage the facts, then upsert everything in one transaction."""
    facts = read_curated()
    properties = pd.read_csv(PROJECT_ROOT / 'data/staging/properties.csv', dtype=str)
    rows = facts[FACT_COLUMNS].astype(object).where(facts[FACT_COLUMNS].notna(), None)
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute((SQL_DIR / '01_schema.sql').read_text())
            cursor.executemany('''
                INSERT INTO mart.dim_property VALUES (%s, %s, %s)
                ON CONFLICT (property_id) DO UPDATE SET
                    property_name = EXCLUDED.property_name, city = EXCLUDED.city
            ''', properties[['property_id', 'property_name', 'city']].itertuples(index=False, name=None))
            cursor.executemany('''
                INSERT INTO mart.dim_month VALUES (%s) ON CONFLICT (month) DO NOTHING
            ''', [(month,) for month in sorted(facts['month'].unique())])
            # This temporary table disappears when the transaction finishes.
            cursor.execute('''CREATE TEMP TABLE stage_property_month ON COMMIT DROP AS
                SELECT property_id, month, total_units, occupied_units, earned_monthly_rent_usd,
                       resolved_orders, sla_eligible_orders, sla_compliant_orders, sla_noncompliant_orders,
                       missing_response_orders, open_orders, energy_kwh, floor_area_sqm
                FROM mart.fact_property_month WITH NO DATA''')
            cursor.executemany('''INSERT INTO stage_property_month VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', rows.itertuples(index=False, name=None))
            cursor.execute((SQL_DIR / '02_load.sql').read_text())
            cursor.execute('SELECT COUNT(*) FROM mart.fact_property_month')
            count = cursor.fetchone()[0]
            if count != 72:
                raise ValueError('Expected 72 fact rows; the transaction was rolled back.')
    return count


def query_table(connection, sql):
    """Return query results as a small table for display or evidence."""
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return pd.DataFrame(cursor.fetchall(), columns=[col.name for col in cursor.description])


def verify_mart():
    """Compare every stored metric, including generated rates, with Spark Parquet."""
    expected = read_curated().sort_values(['property_id', 'month']).reset_index(drop=True)
    with connect() as connection:
        actual = query_table(connection, 'SELECT * FROM mart.fact_property_month ORDER BY property_id, month')
    actual = actual[expected.columns]
    for column in expected.columns:
        if column not in ['property_id', 'month']:
            expected[column] = pd.to_numeric(expected[column])
            actual[column] = pd.to_numeric(actual[column])
    # PostgreSQL stores currency to cents; tiny Spark floating-point differences are allowed.
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False, check_categorical=False,
                                  check_exact=False, rtol=1e-9, atol=1e-7)
    return f'{len(actual)} rows reconciled with Spark Parquet.'


def save_query_results():
    """Save the five business answers and schema information as readable text."""
    output = PROJECT_ROOT / 'evidence/topic4'
    output.mkdir(parents=True, exist_ok=True)
    statements = (SQL_DIR / '03_analytics.sql').read_text().split(';')
    with connect() as connection:
        for number, sql in enumerate(statements[:5], start=1):
            table = query_table(connection, sql)
            (output / f'query_{number}.txt').write_text(table.to_string(index=False) + '\n')
        schema = query_table(connection, '''SELECT table_name, constraint_name, constraint_type
            FROM information_schema.table_constraints WHERE table_schema = 'mart'
            ORDER BY table_name, constraint_name''')
        (output / 'constraints.txt').write_text(schema.to_string(index=False) + '\n')
    return output


if __name__ == '__main__':
    first = load_mart()
    second = load_mart()
    assert first == second == 72, 'Fact counts changed after reloading.'
    print(f'First load: {first}; second load: {second}.')
    print(verify_mart())
    print('Query evidence:', save_query_results())
