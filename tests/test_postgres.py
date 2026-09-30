"""Topic 4: load twice, check the mart, and compare PostgreSQL with Spark.

Uses the database configured in .env; loading requires staging and curated data."""

import unittest
import psycopg2
from src.load_postgres import load_mart, verify_mart, connect, query_table, database_configured
from src.transform import start_spark


@unittest.skipUnless(database_configured(), 'Set DB_* in .env or DATABASE_URL to run PostgreSQL tests.')
class PostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 1. Load the same curated data twice, as in the notebook.
        cls.spark = start_spark()
        cls.addClassCleanup(cls.spark.stop)
        cls.first = load_mart(cls.spark)
        cls.second = load_mart(cls.spark)

    def setUp(self):
        # Close each connection after rolling back any test changes.
        self.connection = connect()
        self.addCleanup(self.connection.close)
        self.addCleanup(self.connection.rollback)

    # 2. Validate row counts, dimensions, and Spark reconciliation.
    def test_reruns_keep_72_rows(self):
        self.assertEqual((self.first, self.second), (72, 72))

    def test_every_metric_matches_spark(self):
        self.assertEqual(verify_mart(self.spark), '72 rows reconciled with Spark Parquet.')

    def test_dimensions_and_portfolio(self):
        table = query_table(self.connection, """SELECT
            (SELECT COUNT(*) FROM mart.dim_property) AS properties,
            (SELECT COUNT(*) FROM mart.dim_month) AS months,
            (SELECT COUNT(*) FROM mart.portfolio_dashboard) AS dashboard_rows""")
        self.assertEqual(table.iloc[0].tolist(), [12, 6, 6])

    # 3. Confirm that the database rejects invalid rows.
    def test_invalid_occupancy_is_rejected(self):
        # The failed transaction rolls back; no test changes remain in the mart.
        with self.assertRaises(psycopg2.errors.CheckViolation):
            with self.connection.cursor() as cursor:
                cursor.execute('UPDATE mart.fact_property_month SET occupied_units = total_units + 1')

    def test_unknown_property_is_rejected(self):
        with self.assertRaises(psycopg2.errors.ForeignKeyViolation):
            with self.connection.cursor() as cursor:
                cursor.execute("UPDATE mart.fact_property_month SET property_id = 'UNKNOWN' WHERE property_id = 'P001'")

    def test_duplicate_key_is_rejected(self):
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            with self.connection.cursor() as cursor:
                cursor.execute("INSERT INTO mart.dim_month VALUES ('2026-01-01')")


if __name__ == '__main__':
    unittest.main()
