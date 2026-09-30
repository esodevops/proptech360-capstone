"""Topic 4: read .env settings before connecting to PostgreSQL."""

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from src import load_postgres


class DatabaseConfigTests(unittest.TestCase):
    def setUp(self):
        # Use a temporary .env so tests never change real credentials.
        self.folder = TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        self.env_file = self.root / '.env'
        self.env_file.write_text('DB_NAME=example_db\nDB_USER=example_user\n')

        # Point the loader at the test folder and isolate terminal settings.
        root_patch = patch.object(load_postgres, 'PROJECT_ROOT', self.root)
        root_patch.start()
        self.addCleanup(root_patch.stop)
        env_patch = patch.dict(os.environ, {}, clear=True)
        env_patch.start()
        self.addCleanup(env_patch.stop)

    def test_password_is_read_exactly(self):
        self.env_file.write_text(
            'DB_NAME=example_db\nDB_USER=example_user\n'
            'DB_PASSWORD="test@pass:${literal}#word"\n'
        )
        # A mock records connection arguments without opening a database.
        with patch.object(load_postgres.psycopg2, 'connect') as connection:
            self.assertTrue(load_postgres.database_configured())
            load_postgres.connect()
            settings = connection.call_args.kwargs
            self.assertEqual(settings['password'], 'test@pass:${literal}#word')
            self.assertEqual(settings['dbname'], 'example_db')

    def test_terminal_settings_override_file(self):
        os.environ['DB_NAME'] = 'terminal_db'
        settings = load_postgres.database_settings()
        self.assertEqual(settings['DB_NAME'], 'terminal_db')

    def test_url_is_supported(self):
        self.env_file.write_text('DATABASE_URL=postgresql+psycopg://user:example@localhost/test\n')
        with patch.object(load_postgres.psycopg2, 'connect') as connection:
            load_postgres.connect()
            connection.assert_called_once_with(
                'postgresql://user:example@localhost/test', connect_timeout=5
            )

    def test_missing_settings_have_clear_error(self):
        self.env_file.write_text('')
        self.assertFalse(load_postgres.database_configured())
        with self.assertRaisesRegex(ValueError, 'Set DB_NAME and DB_USER'):
            load_postgres.connect()


if __name__ == '__main__':
    unittest.main()
