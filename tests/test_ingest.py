"""Run with: python -m unittest discover -s tests -v"""

import hashlib
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from src.ingest import EXPECTED_COLUMNS, ingest_all, ingest_source


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / 'example.csv'

    def test_missing_file(self):
        with self.assertRaisesRegex(FileNotFoundError, 'Source file not found'):
            ingest_source(self.root / 'nonexistent.csv', {'id'})

    def test_missing_column(self):
        self.path.write_text('id\n001\n')
        with self.assertRaisesRegex(ValueError, 'missing required columns: name'):
            ingest_source(self.path, {'id', 'name'})

    def test_empty_inputs(self):
        for content in ['', 'id\n']:
            with self.subTest(content=content):
                self.path.write_text(content)
                with self.assertRaises(ValueError):
                    ingest_source(self.path, {'id'})

    def test_text_values_and_audit(self):
        content = b'id,name\n001,\n002,"Smith, Alex"\n'
        self.path.write_bytes(content)
        frame, entry = ingest_source(self.path, {'id', 'name'})
        self.assertEqual(frame['id'].tolist(), ['001', '002'])
        self.assertEqual(frame['name'].tolist(), ['', 'Smith, Alex'])
        self.assertTrue(all(str(dtype) == 'string' for dtype in frame.dtypes))
        self.assertEqual(entry['filename'], 'example.csv')
        self.assertEqual(entry['row_count'], 2)
        self.assertEqual(entry['sha256'], hashlib.sha256(content).hexdigest())
        timestamp = datetime.fromisoformat(entry['ingest_timestamp_utc'])
        self.assertEqual(timestamp.utcoffset().total_seconds(), 0)

    def make_sources(self):
        raw = self.root / 'raw'
        raw.mkdir()
        for filename, columns in EXPECTED_COLUMNS.items():
            header = sorted(columns)
            (raw / filename).write_text(','.join(header) + '\n' + ','.join(['001'] * len(header)) + '\n')
        return raw

    def test_repeated_runs_preserve_sources(self):
        raw = self.make_sources()
        before = {p.name: p.read_bytes() for p in raw.iterdir()}
        output = self.root / 'curated' / 'manifest.json'
        first_data, first = ingest_all(raw, output)
        second_data, second = ingest_all(raw, output)
        self.assertEqual(before, {p.name: p.read_bytes() for p in raw.iterdir()})
        self.assertEqual(len(json.loads(output.read_text())), 5)
        self.assertEqual([e['sha256'] for e in first], [e['sha256'] for e in second])
        for name in EXPECTED_COLUMNS:
            self.assertTrue(first_data[name].equals(second_data[name]))

    def test_failed_run_preserves_previous_manifest(self):
        raw = self.make_sources()
        output = self.root / 'manifest.json'
        ingest_all(raw, output)
        previous = output.read_bytes()
        (raw / 'units.csv').write_text('wrong_column\n001\n')
        with self.assertRaises(ValueError):
            ingest_all(raw, output)
        self.assertEqual(output.read_bytes(), previous)

    def test_manifest_cannot_overwrite_raw(self):
        raw = self.make_sources()
        source = raw / 'properties.csv'
        before = source.read_bytes()
        with self.assertRaisesRegex(ValueError, 'outside the raw'):
            ingest_all(raw, source)
        self.assertEqual(source.read_bytes(), before)


if __name__ == '__main__':
    unittest.main()
