"""Small Spark examples: rent, occupancy, maintenance, energy, and edge cases."""

import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch
from datetime import date
from pyspark.sql import SparkSession
from src.transform import (SCHEMAS, check_ids, make_calendar, lease_snapshot,
                           aggregate_kpis, join_kpis, check_output, save_kpis)


class TransformationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master('local[2]').appName('Topic3-tests').config('spark.sql.shuffle.partitions', '2').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')
        rows = {
            'properties': [('P1', 'Tower', 'Lagos'), ('P2', 'Empty', 'Abuja')],
            'units': [('U1', 'P1', 100.0, 1), ('U2', 'P1', 50.0, 1), ('U3', 'P1', 50.0, 0), ('U4', 'P2', 0.0, 0)],
            'leases': [('L1', 'U1', date(2026, 1, 1), date(2026, 1, 31), 500.0)],
            'work_orders': [
                ('W1', 'U1', date(2026, 1, 1), date(2026, 1, 2), 'HIGH', 12.0, 20.0, 'RESOLVED', False, True),
                ('W2', 'U1', date(2026, 1, 3), date(2026, 1, 4), 'HIGH', None, 20.0, 'RESOLVED', True, False),
                ('W3', 'U1', date(2026, 1, 5), None, 'LOW', None, 20.0, 'OPEN', True, False),
                ('W4', 'U1', date(2026, 2, 5), None, 'LOW', None, 20.0, 'OPEN', True, False),
            ],
            'meter_readings': [('R1', 'U1', date(2026, 1, 1), 100.0), ('R2', 'U2', date(2026, 1, 1), 50.0)],
        }
        cls.tables = {name: cls.spark.createDataFrame(data, SCHEMAS[name]) for name, data in rows.items()}
        cls.calendar = make_calendar(cls.spark)
        snapshot = lease_snapshot(cls.tables, cls.calendar)
        cls.result = join_kpis(cls.tables, cls.calendar, *aggregate_kpis(cls.tables, snapshot)).cache()
        # Collect only this tiny 12-row test result, never production fact tables.
        cls.output = {(r.property_id, r.month.month): r for r in cls.result.collect()}

    @classmethod
    def tearDownClass(cls):
        cls.result.unpersist()
        cls.spark.stop()

    def test_keys_and_rates(self):
        check_output(self.result, expected_rows=12)

    def test_rent_is_not_multiplied_by_orders(self):
        january = self.output['P1', 1]
        self.assertEqual(january.earned_monthly_rent_usd, 500)
        self.assertEqual(january.occupied_units, 1)
        self.assertEqual(january.total_units, 2)
        self.assertEqual(january.occupancy_rate, 0.5)
        self.assertEqual(self.output['P1', 2].earned_monthly_rent_usd, 0)

    def test_sla_missing_response_and_open_orders(self):
        january = self.output['P1', 1]
        self.assertEqual(january.resolved_orders, 2)
        self.assertEqual(january.sla_eligible_orders, 2)
        self.assertEqual(january.sla_compliant_orders, 1)
        self.assertEqual(january.sla_noncompliant_orders, 1)
        self.assertEqual(january.missing_response_orders, 1)
        self.assertEqual(january.open_orders, 1)
        self.assertEqual(january.sla_compliance_rate, 0.5)
        self.assertIsNone(self.output['P1', 2].sla_compliance_rate)

    def test_energy_includes_all_unit_area(self):
        january = self.output['P1', 1]
        self.assertEqual(january.energy_kwh, 150)
        self.assertEqual(january.floor_area_sqm, 200)
        self.assertEqual(january.energy_intensity_kwh_sqm, 0.75)

    def test_zero_denominators(self):
        empty = self.output['P2', 1]
        self.assertEqual(empty.total_units, 0)
        self.assertIsNone(empty.occupancy_rate)
        self.assertIsNone(empty.energy_intensity_kwh_sqm)

    def test_parquet_reruns_replace_output(self):
        with tempfile.TemporaryDirectory() as folder:
            with patch('src.transform.PROJECT_ROOT', Path(folder)):
                save_kpis(self.result)
                save_kpis(self.result)
            output = Path(folder) / 'data/curated/property_month_kpis'
            saved = self.spark.read.parquet(str(output))
            self.assertEqual(saved.count(), 12)
            self.assertEqual(len(list(output.glob('month=*'))), 6)
            self.assertEqual(saved.select('property_id', 'month').distinct().count(), 12)

    def test_duplicate_identifiers_fail(self):
        units = self.tables['units']
        with self.assertRaisesRegex(ValueError, 'Duplicate unit_id'):
            check_ids(units.union(units), 'unit_id')

    def test_overlapping_leases_fail(self):
        tables = self.tables.copy()
        tables['leases'] = tables['leases'].union(tables['leases'])
        with self.assertRaisesRegex(ValueError, 'Overlapping leases'):
            lease_snapshot(tables, self.calendar)


if __name__ == '__main__':
    unittest.main()
