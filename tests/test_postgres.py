"""Integration checks. Set DATABASE_URL to a proptech360 test database to run."""

import os
import unittest
import psycopg
from src.load_postgres import load_mart, verify_mart, connect, query_table, database_configured


@unittest.skipUnless(database_configured(), 'Set DB_* in .env or DATABASE_URL to run PostgreSQL tests.')
class PostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.first = load_mart()
        cls.second = load_mart()

    def test_reruns_keep_72_rows(self):
        self.assertEqual((self.first, self.second), (72, 72))

    def test_every_metric_matches_spark(self):
        self.assertEqual(verify_mart(), '72 rows reconciled with Spark Parquet.')

    def test_dimensions_and_portfolio(self):
        with connect() as connection:
            table = query_table(connection, '''SELECT
                (SELECT COUNT(*) FROM mart.dim_property) AS properties,
                (SELECT COUNT(*) FROM mart.dim_month) AS months,
                (SELECT COUNT(*) FROM mart.portfolio_dashboard) AS dashboard_rows''')
            self.assertEqual(table.iloc[0].tolist(), [12, 6, 6])

    def test_invalid_occupancy_is_rejected(self):
        # The failed transaction rolls back; no test changes remain in the mart.
        with self.assertRaises(psycopg.errors.CheckViolation):
            with connect() as connection:
                connection.execute('UPDATE mart.fact_property_month SET occupied_units = total_units + 1')

    def test_unknown_property_is_rejected(self):
        with self.assertRaises(psycopg.errors.ForeignKeyViolation):
            with connect() as connection:
                connection.execute("UPDATE mart.fact_property_month SET property_id = 'UNKNOWN' WHERE property_id = 'P001'")

    def test_duplicate_key_is_rejected(self):
        with self.assertRaises(psycopg.errors.UniqueViolation):
            with connect() as connection:
                connection.execute("INSERT INTO mart.dim_month VALUES ('2026-01-01')")


if __name__ == '__main__':
    unittest.main()
