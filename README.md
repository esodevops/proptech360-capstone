# PropTech 360 capstone

## Topic 1 — Python ingestion and reproducibility

The loader reads the five CSV files in `data/raw/` and returns one pandas
DataFrame per file. It checks that each file exists, has the required columns,
and contains data rows. Pandas handles CSV parsing; extra columns are allowed.
This intake step does not enforce strict field counts or reject duplicate headers.

All values are loaded as strings, including IDs and dates. Empty fields remain
empty strings. Cleaning, number/date conversion, and business validation belong
to Topic 2. The intentionally dirty source records are preserved.

### Setup and run

From this project folder, create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m src.ingest
```

Python 3.12 or newer is supported by this code. Topic 1 uses pandas 2.3.3;
all other imports are from the Python standard library.

The output is `data/curated/ingestion_manifest.json`. Each entry records:

- `filename`: the source CSV name.
- `row_count`: the number of loaded records, excluding the header and blank lines.
- `ingest_timestamp_utc`: the time the file was ingested, with UTC offset `+00:00`.
- `sha256`: a fingerprint of the exact source bytes that were loaded.

Running again replaces the manifest instead of appending duplicate entries.
Timestamps change each run, but unchanged source files have the same hashes,
row counts, and loaded values. The loader only reads raw files and prevents
saving the manifest inside the raw directory. All five files must validate
before the manifest is written; a validation failure keeps the previous manifest.

### Use in Python or the notebook

The Topic 1 cell in `capstone.ipynb` calls the same reusable code. Open the
notebook from the project root and select a kernel with the requirements installed.
The command-line workflow above does not require Jupyter.

```python
from src.ingest import ingest_all

datasets, ingestion_manifest = ingest_all()
units = datasets['units.csv']
```

To load a single file, use `ingest_source(path, expected_columns)`. It returns
`(dataset, manifest_entry)`. A missing file raises `FileNotFoundError`; invalid
CSV structure or missing columns raise `ValueError` with the filename and reason.
The command-line entry point reports the error and exits with a failure status.

### Run the tests

```bash
python -m unittest discover -s tests -v
```

The tests create temporary fixtures, so they never modify the project source
CSVs. They check controlled failure for a nonexistent path, missing columns,
empty inputs, stable text values, audit metadata, and safe
repeated runs. They also check that failed validation preserves the previous
manifest and that the manifest cannot overwrite raw inputs.

Verified locally with Python 3.14.7 and pandas 2.3.3: all 7 tests passed.
The Topic 1 notebook cell also ran successfully. Two consecutive ingestion runs
returned identical data and preserved every raw file's SHA-256 digest.
The supplied sources contain 12 properties, 241 units, 198 leases, 1,501 work
orders, and 1,440 meter readings (including the intentional quality defects).

### Short explanation for reviewers

1. **Read:** load each CSV as text so IDs and empty values stay unchanged.
2. **Check:** raise a clear error for missing files, required columns, or data rows.
3. **Record:** capture the filename, row count, UTC time, and SHA-256 fingerprint.
4. **Save:** replace the audit manifest only after all five files pass the checks.

`ingest_source()` handles one file. `ingest_all()` repeats that process for the
five files and saves the manifest. `BytesIO` lets pandas read the same bytes
used for the fingerprint, so the audit describes exactly what was loaded.
