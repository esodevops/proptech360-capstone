"""Build one KPI row per property and month from Topic 2 staging files."""

from pyspark.sql import SparkSession, functions as F
from src.ingest import PROJECT_ROOT

# Schemas tell Spark which values are text, numbers, dates, and booleans.
SCHEMAS = {
    'properties': 'property_id STRING, property_name STRING, city STRING',
    'units': 'unit_id STRING, property_id STRING, floor_area_sqm DOUBLE, active INT',
    'leases': 'lease_id STRING, unit_id STRING, start_date DATE, end_date DATE, monthly_rent_usd DOUBLE',
    'work_orders': 'work_order_id STRING, unit_id STRING, opened_date DATE, closed_date DATE, priority STRING, response_hours DOUBLE, cost_usd DOUBLE, status STRING, missing_response BOOLEAN, sla_compliant BOOLEAN',
    'meter_readings': 'reading_id STRING, unit_id STRING, reading_month DATE, energy_kwh DOUBLE',
}
ID_COLUMNS = {'properties': 'property_id', 'units': 'unit_id', 'leases': 'lease_id',
              'work_orders': 'work_order_id', 'meter_readings': 'reading_id'}


def load_staging(spark):
    """Read validated files and stop if duplicate or missing IDs remain."""
    tables = {}
    for name, schema in SCHEMAS.items():
        path = PROJECT_ROOT / 'data' / 'staging' / (name + '.csv')
        tables[name] = spark.read.schema(schema).option('header', True).option('mode', 'FAILFAST').csv(str(path))
        check_ids(tables[name], ID_COLUMNS[name])
    return tables


def check_ids(table, identifier):
    if table.filter(F.col(identifier).isNull() | (F.trim(F.col(identifier)) == '')).count():
        raise ValueError(f'Missing {identifier}; rerun Topic 2.')
    if table.groupBy(identifier).count().filter('count > 1').count():
        raise ValueError(f'Duplicate {identifier}; rerun Topic 2.')


def make_calendar(spark):
    """Create January through June, including each month's final date."""
    dates = [(f'2026-{month:02d}-01',) for month in range(1, 7)]
    return spark.createDataFrame(dates, ['month']).withColumn('month', F.to_date('month')).withColumn('month_end', F.last_day('month'))


def lease_snapshot(tables, calendar):
    """Keep one row per active unit per month, even when it has no lease."""
    units = tables['units'].filter('active = 1').crossJoin(calendar)
    leases = tables['leases']
    joined = units.join(leases, 'unit_id', 'left')
    current = joined.filter((F.col('start_date') <= F.col('month_end')) &
                            (F.col('end_date').isNull() | (F.col('end_date') >= F.col('month_end'))))
    if current.groupBy('unit_id', 'month').count().filter('count > 1').count():
        raise ValueError('Overlapping leases at month end; rerun Topic 2.')
    current = current.select('unit_id', 'month', 'monthly_rent_usd').withColumn('occupied', F.lit(1))
    return units.join(current, ['unit_id', 'month'], 'left').fillna({'occupied': 0, 'monthly_rent_usd': 0})


def aggregate_kpis(tables, snapshot):
    """Aggregate each fact table separately to prevent multiplied totals."""
    occupancy = snapshot.groupBy('property_id', 'month').agg(
        F.count('*').alias('total_units'), F.sum('occupied').alias('occupied_units'),
        F.sum('monthly_rent_usd').alias('earned_monthly_rent_usd'))

    orders = tables['work_orders'].join(tables['units'].select('unit_id', 'property_id'), 'unit_id')
    orders = orders.withColumn('month', F.trunc('opened_date', 'month'))
    # Missing response hours never count as compliant.
    limit = F.when(F.col('priority') == 'CRITICAL', 4).when(F.col('priority') == 'HIGH', 12).when(F.col('priority') == 'MEDIUM', 48).when(F.col('priority') == 'LOW', 72)
    resolved = F.col('status') == 'RESOLVED'
    compliant = resolved & (F.col('response_hours') >= 0) & (F.col('response_hours') <= limit)
    maintenance = orders.groupBy('property_id', 'month').agg(
        F.sum(F.when(resolved, 1).otherwise(0)).alias('resolved_orders'),
        F.sum(F.when(compliant, 1).otherwise(0)).alias('sla_compliant_orders'),
        F.sum(F.when(resolved & F.col('response_hours').isNull(), 1).otherwise(0)).alias('missing_response_orders'),
        F.sum(F.when(F.col('status') == 'OPEN', 1).otherwise(0)).alias('open_orders'))
    maintenance = maintenance.withColumn('sla_eligible_orders', F.col('resolved_orders'))
    maintenance = maintenance.withColumn('sla_noncompliant_orders', F.col('resolved_orders') - F.col('sla_compliant_orders'))

    readings = tables['meter_readings'].join(tables['units'].select('unit_id', 'property_id'), 'unit_id')
    readings = readings.withColumn('month', F.trunc('reading_month', 'month'))
    energy = readings.groupBy('property_id', 'month').agg(F.sum('energy_kwh').alias('energy_kwh'))
    area = tables['units'].groupBy('property_id').agg(F.sum('floor_area_sqm').alias('floor_area_sqm'))
    return occupancy, maintenance, energy, area


def join_kpis(tables, calendar, occupancy, maintenance, energy, area):
    """Start with every property-month so properties with no activity remain."""
    result = tables['properties'].select('property_id').crossJoin(calendar.select('month'))
    for table in [occupancy, maintenance, energy]:
        result = result.join(table, ['property_id', 'month'], 'left')
    result = result.join(area, 'property_id', 'left')
    totals = ['total_units', 'occupied_units', 'earned_monthly_rent_usd', 'resolved_orders',
              'sla_eligible_orders', 'sla_compliant_orders', 'sla_noncompliant_orders', 'missing_response_orders', 'open_orders', 'energy_kwh']
    result = result.fillna(0, subset=totals)
    # A rate is unknown when its denominator is zero or missing.
    for output, numerator, denominator in [
        ('occupancy_rate', 'occupied_units', 'total_units'),
        ('sla_compliance_rate', 'sla_compliant_orders', 'sla_eligible_orders'),
        ('energy_intensity_kwh_sqm', 'energy_kwh', 'floor_area_sqm'),
    ]:
        result = result.withColumn(output, F.when(F.col(denominator) > 0, F.col(numerator) / F.col(denominator)))
    return result


def check_output(result, expected_rows=72):
    """Check the output size, unique keys, and rate limits before saving."""
    assert result.count() == expected_rows, 'Unexpected property-month row count.'
    assert result.select('property_id', 'month').distinct().count() == expected_rows, 'Duplicate property-month keys.'
    for column in ['occupancy_rate', 'sla_compliance_rate']:
        assert result.filter((F.col(column) < 0) | (F.col(column) > 1)).count() == 0, f'Invalid {column}.'


def save_kpis(result):
    path = PROJECT_ROOT / 'data' / 'curated' / 'property_month_kpis'
    result.write.mode('overwrite').partitionBy('month').parquet(str(path))


def run_transform(spark):
    tables = load_staging(spark)
    calendar = make_calendar(spark)
    snapshot = lease_snapshot(tables, calendar)
    result = join_kpis(tables, calendar, *aggregate_kpis(tables, snapshot)).cache()
    check_output(result)
    save_kpis(result)
    return result


if __name__ == '__main__':
    spark = SparkSession.builder.master('local[2]').appName('PropTech360-Topic3').getOrCreate()
    try:
        run_transform(spark).orderBy('property_id', 'month').show(6, truncate=False)
    finally:
        spark.stop()
