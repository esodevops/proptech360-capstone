"""Check private configuration handling without contacting PostgreSQL."""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from src import load_postgres


class DatabaseConfigTests(unittest.TestCase):
    def test_separate_settings_preserve_special_password_characters(self):
        with TemporaryDirectory() as folder:
            Path(folder, '.env').write_text(
                'DB_NAME=example_db\nDB_USER=example_user\n'
                'DB_PASSWORD="test@pass:${literal}#word"\nDB_HOST=localhost\nDB_PORT=5432\n')
            with patch.object(load_postgres, 'PROJECT_ROOT', Path(folder)):
                with patch.dict('os.environ', {}, clear=True):
                    with patch.object(load_postgres.psycopg, 'connect') as connection:
                        self.assertTrue(load_postgres.database_configured())
                        load_postgres.connect()
                        self.assertEqual(connection.call_args.kwargs['password'], 'test@pass:${literal}#word')
                        self.assertEqual(connection.call_args.kwargs['dbname'], 'example_db')

    def test_terminal_settings_override_file(self):
        with patch.object(load_postgres, 'dotenv_values', return_value={'DB_NAME': 'file_db', 'DB_USER': 'user'}):
            with patch.dict('os.environ', {'DB_NAME': 'terminal_db'}, clear=True):
                self.assertEqual(load_postgres.database_settings()['DB_NAME'], 'terminal_db')

    def test_url_is_supported(self):
        settings = {'DATABASE_URL': 'postgresql+psycopg://user:example@localhost/test'}
        with patch.object(load_postgres, 'database_settings', return_value=settings):
            with patch.object(load_postgres.psycopg, 'connect') as connection:
                load_postgres.connect()
                connection.assert_called_once_with('postgresql://user:example@localhost/test', connect_timeout=5)

    def test_missing_settings_have_clear_error(self):
        with patch.object(load_postgres, 'database_settings', return_value={}):
            self.assertFalse(load_postgres.database_configured())
            with self.assertRaisesRegex(ValueError, 'Set DB_NAME and DB_USER'):
                load_postgres.connect()


if __name__ == '__main__':
    unittest.main()
