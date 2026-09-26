# PropTech 360 capstone

## Topic 1 — Python ingestion and reproducibility

The loader reads the five CSV files in `data/raw/` and returns one pandas
DataFrame per file. It checks that each file exists, has the required columns,
and contains data rows. Pandas handles CSV parsing; extra columns are allowed.
This intake step does not enforce strict field counts or reject duplicate headers.

All values are loaded as strings, including IDs and dates. Empty fields remain
empty strings during ingestion. Topic 2 converts blank fields to missing values
and handles number/date conversion and business validation. The intentionally dirty source records are preserved.

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

## Topic 2 — Pandas validation, cleaning, and quarantine

Run with your existing Python environment activated:

```bash
python -m src.quality
python -m unittest discover -s tests -v
```

The Topic 2 notebook cell calls the same code. No additional packages are needed.
`profile_table()` counts nulls (including whitespace-only fields), duplicate IDs,
and original column types. `validate_and_quarantine()` returns clean tables,
rejected tables, and a summary. `run_quality()` loads the inputs and saves them.

### Validation policies

| Check | Policy |
|---|---|
| Exact duplicate rows | Keep the first copy; quarantine subsequent copies. |
| Conflicting duplicate IDs | After removing exact copies, quarantine all rows sharing an ID. |
| Missing IDs, property names, or cities | Quarantine; do not invent values. |
| Dates | Parse `YYYY-MM-DD`; quarantine invalid nonblank dates and missing required start/opened/reading dates. An empty lease end means open-ended. |
| Numbers | Convert to numeric; quarantine missing, unparseable, infinite, or negative values. Area must also be greater than zero. |
| Categories | Require active `0`/`1`, priority CRITICAL/HIGH/MEDIUM/LOW, and status OPEN/RESOLVED. Otherwise quarantine. |
| Foreign keys | Require accepted properties for units and accepted units for child records. Otherwise quarantine. |
| Date order | Quarantine leases with start after end and work orders with opened after closed. Resolved orders require a closing date; open orders must not have one. |
| Lease overlap | Among otherwise valid leases, quarantine every overlapping lease for a unit. End dates are inclusive; an empty end has no limit. |
| Reading period | Require dates within January–June 2026. |
| Repeated unit-month | After exact duplicates, quarantine all competing readings in the same calendar month, even with different reading IDs or days. |
| Missing response hours | Retain and flag with `missing_response=True`; preserve the missing value. `sla_compliant=False`. Nonblank invalid/negative responses are quarantined. |

For SLA reporting, use accepted RESOLVED work orders as the denominator,
including those missing a response. Count `sla_compliant=True` for the numerator.
OPEN orders do not enter the denominator. Thresholds are 4, 12, 48, and 72 hours
for CRITICAL, HIGH, MEDIUM, and LOW respectively.

### Outputs and reconciliation

- `data/staging/`: five accepted CSVs with parsed dates/numbers and SLA flags.
- `data/quarantine/`: five CSVs containing original field values, semicolon-separated
  `reason_code` values, and a UTC-based `run_id`. Empty files still have headers.
- `data/curated/quality_summary.json`: null counts, types, duplicate counts,
  counts for each rule, and input/accepted/quarantined totals per dataset.

For every table, **input = accepted + quarantined**. Each rejected row is counted
once, even when it breaks several rules. Exact duplicate copies ARE included in
quarantined totals. `duplicate_identifiers` counts ID occurrences after the first;
`exact_duplicates` counts identical copies after the first. These are overlapping
diagnostic counts, not extra rows to add to reconciliation. Rule counts can also
overlap. `MISSING_RESPONSE_RETAINED` is a flag count, not a rejection count.

The raw CSVs and supplied DataFrames remain unchanged. Reruns replace the latest
outputs; they do not append rows. Run IDs change while row results stay the same
for unchanged inputs. Invalid input files/columns fail in Topic 1 before validation.
The overlap check uses simple per-unit comparisons suited to this small capstone.

### Short explanation for reviewers

1. **Check:** test each row against clear rules and collect its reasons.
2. **Split:** keep valid rows; quarantine original invalid rows with explanations.
3. **Count:** show that every input row is either accepted or quarantined.
4. **Save:** write staging, quarantine, and summary files without editing raw data.

### Presenting the Topic 2 code

The code works in four steps:

1. **Profile:** `profile_table()` counts missing values and duplicate IDs so we
   know what arrived.
2. **Check:** `check_row()` uses `if` statements to add problem names to a list.
   Dates and numbers are converted first so comparisons work correctly.
3. **Separate:** `validate_and_quarantine()` puts rows without problems in the
   clean table. It saves the original rejected rows with their problem names.
   Parent records are checked first so child records cannot reference rejected parents.
4. **Save:** `run_quality()` writes separate output files. This protects the raw data
   and makes running the process again safe.

Blank text becomes missing when dates/numbers are converted. Surrounding spaces
are removed from working text values because they are formatting differences;
original rejected values stay unchanged. Missing optional dates and response
hours are allowed, but nonblank invalid values are rejected. The summary lists
rules that found problems; an absent rule count means zero failures.

Run the solution in your activated environment:

```bash
python -m src.quality
```

This runs the module and saves clean data, quarantine data, and summary counts.
To check that it behaves correctly:

```bash
python -m unittest discover -s tests
```

This finds and runs the project tests. It does not edit the raw files.

## Topic 3 — PySpark curated transformations

The Topic 3 notebook cell keeps the starter setup and implements each TODO by
calling a small function in `src/transform.py`. It reads Topic 2 staging CSVs,
not raw CSVs. Run Topic 2 first when inputs change.

### What each block does

1. **Load:** explicit schemas tell Spark which columns contain dates and numbers.
   Missing or duplicate IDs stop the run with a message to rerun Topic 2.
2. **Calendar:** create six months and their month-end dates.
3. **Snapshot:** create one row for each active unit and month. Attach the lease
   active on the last day; no lease means zero occupied units and zero rent.
   Multiple active leases for one unit-month stop the run rather than guessing
   which rent to use. Topic 2 already quarantines overlapping leases.
4. **Aggregate:** calculate occupancy/rent, maintenance, and energy separately.
   Joining these totals prevents multiple work orders from multiplying rent.
5. **Join and check:** keep every property for every month, including properties
   without activity. Check 72 rows, unique property/month keys, and rates from 0 to 1.
6. **Save:** write Parquet files into month folders. Overwrite replaces the previous
   output, making reruns safe without appending duplicate records.

### Rate and date policies

- Occupancy uses **active units** as the denominator. Leases ending on the last
  day count as occupied. Rent is the month-end contractual rent for those units.
- Maintenance belongs to the month of `opened_date`, even when closed later.
  All accepted resolved cases are SLA eligible, including missing responses.
  Missing responses count as noncompliant. Open cases are reported separately.
- Energy uses **all accepted unit floor area**, including inactive and vacant units.
  It sums the accepted readings recorded in that month. No readings means zero
  recorded energy, which does not prove actual consumption was zero.
- Zero or missing denominators produce null (blank) rates, not divide-by-zero errors.
  Ratios are fractions: `0.5` means 50%. Non-positive unit areas are rejected in Topic 2.
- No window ranking is needed: the unit-month check rejects competing leases,
  so there is no arbitrary “winning” lease to select.

### Run

With your project environment activated:

```bash
python -m src.transform
```

This starts Spark locally, builds the KPIs, validates them, and writes
`data/curated/property_month_kpis/`, partitioned by `month`.
The standalone run does not require a PostgreSQL driver. The notebook preserves
your existing driver configuration for later database work.

```bash
python -m unittest discover -s tests -p 'test_transform.py'
```

This runs small Spark tests for rent multiplication, month-end occupancy, missing
SLA responses, unresolved cases, floor area, zero denominators, duplicate IDs,
and overlapping leases. Use the pinned dependencies in `requirements.txt` and
a compatible Java installation. Spark needs permission to open local sockets.

Verified with the installed PySpark 4.2.0 and Java 17: the full staging run produced
72 unique property-month rows across six Parquet partitions. All 17 earlier tests
and eight Spark tests passed. The Parquet test writes twice and reads the files
back to verify that overwrite does not duplicate rows.
