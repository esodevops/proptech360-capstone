"""Small examples for each Topic 2 policy."""

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
        self.data = {name: pd.DataFrame([row], dtype='string') for name, row in rows.items()}

    def run_checks(self):
        return quality.validate_and_quarantine(self.data, 'test-run')

    def test_clean_data(self):
        clean, rejected, summary = self.run_checks()
        self.assertTrue(all(len(table) == 1 for table in clean.values()))
        self.assertTrue(all(table.empty for table in rejected.values()))
        self.assertTrue(clean['work_orders.csv'].iloc[0]['sla_compliant'])
        self.assertEqual(summary['leases.csv']['nulls']['end_date'], 1)

    def test_exact_duplicate(self):
        name = 'units.csv'
        self.data[name] = pd.concat([self.data[name], self.data[name]], ignore_index=True)
        clean, rejected, summary = self.run_checks()
        self.assertEqual(len(clean[name]), 1)
        self.assertEqual(rejected[name].iloc[0]['reason_code'], 'EXACT_DUPLICATE')
        self.assertEqual(summary[name]['exact_duplicates'], 1)
        self.assertEqual(len(clean['leases.csv']), 1)

    def test_conflicting_ids_and_parent_rejection(self):
        name = 'units.csv'
        other = self.data[name].copy()
        other['floor_area_sqm'] = '60'
        self.data[name] = pd.concat([self.data[name], other], ignore_index=True)
        clean, rejected, _ = self.run_checks()
        self.assertTrue(clean[name].empty)
        self.assertTrue(rejected[name]['reason_code'].str.contains('DUPLICATE_ID').all())
        self.assertIn('UNKNOWN_UNIT', rejected['leases.csv'].iloc[0]['reason_code'])

    def test_foreign_keys(self):
        self.data['units.csv'].loc[0, 'property_id'] = 'unknown'
        _, rejected, _ = self.run_checks()
        self.assertIn('UNKNOWN_PROPERTY', rejected['units.csv'].iloc[0]['reason_code'])
        self.assertIn('UNKNOWN_UNIT', rejected['meter_readings.csv'].iloc[0]['reason_code'])

    def test_negative_invalid_and_zero_numbers(self):
        for name, column, value in [('meter_readings.csv', 'energy_kwh', '-1'),
                                    ('units.csv', 'floor_area_sqm', '0'),
                                    ('leases.csv', 'monthly_rent_usd', 'oops'),
                                    ('work_orders.csv', 'cost_usd', 'inf')]:
            self.data[name].loc[0, column] = value
        _, rejected, _ = self.run_checks()
        for name in ['meter_readings.csv', 'units.csv', 'leases.csv', 'work_orders.csv']:
            self.assertEqual(len(rejected[name]), 1)

    def test_missing_response(self):
        self.data['work_orders.csv'].loc[0, 'response_hours'] = ''
        clean, _, summary = self.run_checks()
        row = clean['work_orders.csv'].iloc[0]
        self.assertTrue(row['missing_response'])
        self.assertFalse(row['sla_compliant'])
        self.assertTrue(pd.isna(row['response_hours']))
        self.assertEqual(summary['work_orders.csv']['rule_counts']['MISSING_RESPONSE_RETAINED'], 1)

    def test_dates_and_categories(self):
        self.data['leases.csv'].loc[0, 'start_date'] = 'bad-date'
        self.data['work_orders.csv'].loc[0, 'closed_date'] = '2025-12-01'
        self.data['work_orders.csv'].loc[0, 'priority'] = 'URGENT'
        self.data['work_orders.csv'].loc[0, 'status'] = 'INVALID'
        self.data['meter_readings.csv'].loc[0, 'reading_month'] = '2026-07-01'
        _, rejected, _ = self.run_checks()
        self.assertIn('INVALID_START_DATE', rejected['leases.csv'].iloc[0]['reason_code'])
        codes = rejected['work_orders.csv'].iloc[0]['reason_code']
        for code in ['WORK_ORDER_DATE_ORDER', 'INVALID_PRIORITY', 'INVALID_STATUS']:
            self.assertIn(code, codes)
        self.assertIn('READING_OUT_OF_RANGE', rejected['meter_readings.csv'].iloc[0]['reason_code'])

    def test_lease_order_and_overlap(self):
        self.data['leases.csv'].loc[0, 'end_date'] = '2025-01-01'
        _, rejected, _ = self.run_checks()
        self.assertIn('LEASE_DATE_ORDER', rejected['leases.csv'].iloc[0]['reason_code'])
        self.data['leases.csv'].loc[0, 'end_date'] = '2026-02-01'
        other = self.data['leases.csv'].copy()
        other.loc[0, ['lease_id', 'start_date', 'end_date']] = ['L2', '2026-02-01', '']
        self.data['leases.csv'] = pd.concat([self.data['leases.csv'], other], ignore_index=True)
        clean, rejected, _ = self.run_checks()
        self.assertTrue(clean['leases.csv'].empty)
        self.assertTrue(rejected['leases.csv']['reason_code'].str.contains('OVERLAPPING_LEASE').all())

    def test_duplicate_calendar_month(self):
        name = 'meter_readings.csv'
        other = self.data[name].copy()
        other.loc[0, ['reading_id', 'reading_month']] = ['R2', '2026-01-15']
        self.data[name] = pd.concat([self.data[name], other], ignore_index=True)
        clean, rejected, _ = self.run_checks()
        self.assertTrue(clean[name].empty)
        self.assertTrue(rejected[name]['reason_code'].str.contains('DUPLICATE_UNIT_MONTH').all())

    def test_original_rows_reconciliation_and_reruns(self):
        self.data['meter_readings.csv'].loc[0, 'energy_kwh'] = '-001'
        before = {name: table.copy() for name, table in self.data.items()}
        first, rejected, summary = self.run_checks()
        second, _, _ = self.run_checks()
        self.assertEqual(rejected['meter_readings.csv'].iloc[0]['energy_kwh'], '-001')
        self.assertEqual(rejected['meter_readings.csv'].iloc[0]['run_id'], 'test-run')
        for name, counts in summary.items():
            self.assertEqual(counts['input'], counts['accepted'] + counts['quarantined'])
            pd.testing.assert_frame_equal(before[name], self.data[name])
            pd.testing.assert_frame_equal(first[name], second[name])


if __name__ == '__main__':
    unittest.main()
