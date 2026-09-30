"""Topic 2: profile, validate, quarantine, and reconcile small Pandas tables."""

import unittest
import pandas as pd
from src import quality


class QualityTests(unittest.TestCase):
    def setUp(self):
        rows = {
            'properties.csv': dict(property_id='P1', property_name='Tower', city='Lagos'),
            'units.csv': dict(unit_id='U1', property_id='P1', floor_area_sqm='50', active='1'),
            'leases.csv': dict(lease_id='L1', unit_id='U1', start_date='2026-01-01', end_date='', monthly_rent_usd='500'),
            'work_orders.csv': dict(work_order_id='W1', unit_id='U1', opened_date='2026-01-01', closed_date='2026-01-02', priority='HIGH', response_hours='4', cost_usd='20', status='RESOLVED'),
            'meter_readings.csv': dict(reading_id='R1', unit_id='U1', reading_month='2026-01-01', energy_kwh='100'),
        }
        self.datasets = {}
        for name, row in rows.items():
            self.datasets[name] = pd.DataFrame([row])

    # Run the notebook validation on the small example tables.
    def run_checks(self):
        return quality.validate_and_quarantine(self.datasets, 'test-run')

    def test_clean_data(self):
        clean, rejected, summary = self.run_checks()
        for name in self.datasets:
            self.assertEqual(len(clean[name]), 1)
            self.assertTrue(rejected[name].empty)
        self.assertTrue(clean['work_orders.csv'].iloc[0]['sla_compliant'])
        self.assertEqual(summary['leases.csv']['nulls']['end_date'], 1)

    # Duplicate work orders must not be counted twice.
    def test_exact_duplicate(self):
        name = 'work_orders.csv'
        self.datasets[name] = pd.concat([self.datasets[name], self.datasets[name]], ignore_index=True)
        clean, rejected, summary = self.run_checks()
        self.assertEqual(len(clean[name]), 1)
        self.assertEqual(rejected[name].iloc[0]['reason_code'], 'EXACT_DUPLICATE')
        self.assertEqual(summary[name]['exact_duplicates'], 1)
        self.assertEqual(len(clean['leases.csv']), 1)

    def test_conflicting_ids_and_parent_rejection(self):
        name = 'units.csv'
        other = self.datasets[name].copy()
        other['floor_area_sqm'] = '60'
        self.datasets[name] = pd.concat([self.datasets[name], other], ignore_index=True)
        clean, rejected, _ = self.run_checks()
        self.assertTrue(clean[name].empty)
        self.assertTrue(rejected[name]['reason_code'].str.contains('DUPLICATE_ID').all())
        self.assertIn('UNKNOWN_UNIT', rejected['leases.csv'].iloc[0]['reason_code'])

    def test_foreign_keys(self):
        self.datasets['units.csv'].loc[0, 'property_id'] = 'unknown'
        _, rejected, _ = self.run_checks()
        self.assertIn('UNKNOWN_PROPERTY', rejected['units.csv'].iloc[0]['reason_code'])
        self.assertIn('UNKNOWN_UNIT', rejected['meter_readings.csv'].iloc[0]['reason_code'])

    def test_unknown_unit(self):
        self.datasets['work_orders.csv'].loc[0, 'unit_id'] = 'UNKNOWN'
        clean, rejected, _ = self.run_checks()
        self.assertTrue(clean['work_orders.csv'].empty)
        self.assertIn('UNKNOWN_UNIT', rejected['work_orders.csv'].iloc[0]['reason_code'])

    def test_negative_energy(self):
        self.datasets['meter_readings.csv'].loc[0, 'energy_kwh'] = '-1'
        _, rejected, _ = self.run_checks()
        self.assertIn('INVALID_ENERGY_KWH', rejected['meter_readings.csv'].iloc[0]['reason_code'])

    def test_zero_area(self):
        self.datasets['units.csv'].loc[0, 'floor_area_sqm'] = '0'
        _, rejected, _ = self.run_checks()
        self.assertIn('ZERO_AREA', rejected['units.csv'].iloc[0]['reason_code'])

    def test_invalid_rent(self):
        self.datasets['leases.csv'].loc[0, 'monthly_rent_usd'] = 'oops'
        _, rejected, _ = self.run_checks()
        self.assertIn('INVALID_MONTHLY_RENT_USD', rejected['leases.csv'].iloc[0]['reason_code'])

    def test_infinite_cost(self):
        self.datasets['work_orders.csv'].loc[0, 'cost_usd'] = 'inf'
        _, rejected, _ = self.run_checks()
        self.assertIn('INVALID_COST_USD', rejected['work_orders.csv'].iloc[0]['reason_code'])

    def test_missing_response(self):
        self.datasets['work_orders.csv'].loc[0, 'response_hours'] = ''
        clean, _, summary = self.run_checks()
        row = clean['work_orders.csv'].iloc[0]
        self.assertTrue(row['missing_response'])
        self.assertFalse(row['sla_compliant'])
        self.assertTrue(pd.isna(row['response_hours']))
        self.assertEqual(summary['work_orders.csv']['rule_counts']['MISSING_RESPONSE_RETAINED'], 1)

    def test_dates_and_categories(self):
        self.datasets['leases.csv'].loc[0, 'start_date'] = 'bad-date'
        self.datasets['work_orders.csv'].loc[0, 'closed_date'] = '2025-12-01'
        self.datasets['work_orders.csv'].loc[0, 'priority'] = 'URGENT'
        self.datasets['work_orders.csv'].loc[0, 'status'] = 'INVALID'
        self.datasets['meter_readings.csv'].loc[0, 'reading_month'] = '2026-07-01'
        _, rejected, _ = self.run_checks()
        self.assertIn('INVALID_START_DATE', rejected['leases.csv'].iloc[0]['reason_code'])
        codes = rejected['work_orders.csv'].iloc[0]['reason_code']
        for code in ['WORK_ORDER_DATE_ORDER', 'INVALID_PRIORITY', 'INVALID_STATUS']:
            self.assertIn(code, codes)
        self.assertIn('READING_OUT_OF_RANGE', rejected['meter_readings.csv'].iloc[0]['reason_code'])

    def test_lease_order_and_overlap(self):
        self.datasets['leases.csv'].loc[0, 'end_date'] = '2025-01-01'
        _, rejected, _ = self.run_checks()
        self.assertIn('LEASE_DATE_ORDER', rejected['leases.csv'].iloc[0]['reason_code'])
        self.datasets['leases.csv'].loc[0, 'end_date'] = '2026-02-01'
        other = self.datasets['leases.csv'].copy()
        other.loc[0, ['lease_id', 'start_date', 'end_date']] = ['L2', '2026-02-01', '']
        self.datasets['leases.csv'] = pd.concat([self.datasets['leases.csv'], other], ignore_index=True)
        clean, rejected, _ = self.run_checks()
        self.assertTrue(clean['leases.csv'].empty)
        self.assertTrue(rejected['leases.csv']['reason_code'].str.contains('OVERLAPPING_LEASE').all())

    def test_duplicate_calendar_month(self):
        name = 'meter_readings.csv'
        other = self.datasets[name].copy()
        other.loc[0, ['reading_id', 'reading_month']] = ['R2', '2026-01-15']
        self.datasets[name] = pd.concat([self.datasets[name], other], ignore_index=True)
        clean, rejected, _ = self.run_checks()
        self.assertTrue(clean[name].empty)
        self.assertTrue(rejected[name]['reason_code'].str.contains('DUPLICATE_UNIT_MONTH').all())

    # Accepted + quarantined must equal the original number of rows.
    def test_original_rows_reconciliation_and_reruns(self):
        self.datasets['meter_readings.csv'].loc[0, 'energy_kwh'] = '-001'
        before = {name: table.copy() for name, table in self.datasets.items()}
        first, rejected, summary = self.run_checks()
        second, _, _ = self.run_checks()
        self.assertEqual(rejected['meter_readings.csv'].iloc[0]['energy_kwh'], '-001')
        self.assertEqual(rejected['meter_readings.csv'].iloc[0]['run_id'], 'test-run')
        for name, counts in summary.items():
            self.assertEqual(counts['input'], counts['accepted'] + counts['quarantined'])
            pd.testing.assert_frame_equal(before[name], self.datasets[name])
            pd.testing.assert_frame_equal(first[name], second[name])


if __name__ == '__main__':
    unittest.main()
