# PropTech 360 — Property Analytics Pipeline

PropTech 360 turns property management CSV extracts into monthly reports for occupancy, rent, maintenance service levels, and energy consumption. The project uses Python and Pandas to validate the inputs, PySpark to build curated data, PostgreSQL for a dimensional reporting mart, and AWS for file auditing and Athena queries.

The reporting period is **January–June 2026**. The final dataset contains **one row per property per month: 12 properties × 6 months = 72 rows**. All source records are synthetic and include intentional data quality problems.

## Main workflow

```text
Five raw CSV files
        |
        v
Python ingestion + file manifest
        |
        v
Pandas validation ------> Quarantine CSVs + rejection reasons
        |
        v
Accepted staging CSVs
        |
        v
PySpark monthly KPIs
        |
        v
Curated Parquet, partitioned by month
        |                         |
        v                         v
PostgreSQL mart               S3 curated/
        |                         |
        v                         v
SQL analytics                Glue Data Catalog --> Athena

Separate AWS arrival audit:
S3 raw/ upload --> Lambda --> S3 audit/ JSON + CloudWatch logs
```

Lambda audits raw file arrivals. Spark performs the transformations locally, and its actual Parquet output is uploaded separately to S3.

## Project files

| Location | Purpose |
|---|---|
| `capstone.ipynb` | Main notebook: dataset generation and implementations for the five topics. |
| `src/ingest.py` | Read the source files and save ingestion metadata. |
| `src/quality.py` | Profile, validate, clean, quarantine, and reconcile rows. |
| `src/transform.py` | Build and save monthly property KPIs with Spark. |
| `src/load_postgres.py` | Load the PostgreSQL mart, verify metrics, and export analytics. |
| `sql/` | Database creation, schema, upsert, and analytical SQL. |
| `tests/` | Ingestion, validation, Spark, configuration, and database tests. |
| `data/raw/` | Original synthetic CSV extracts. |
| `data/staging/` | Accepted records for Spark. |
| `data/quarantine/` | Rejected records with reasons and run identifiers. |
| `data/curated/` | Ingestion manifest, quality summary, and monthly Parquet output. |
| `evidence/` | Evidence organized by topic; see `evidence/README.md` for the folder index. |
| `aws/lambda_handler.py` | Deployable Lambda handler for auditing raw S3 arrivals. |
| `.env.example` | Example database configuration without real credentials. |
| `requirements.txt` | Pinned Python dependencies. |

The notebook contains the implementations within their topic sections. The Python modules follow the same processing stages and provide a command-line alternative. The notebook does not simply call every module.

## Local setup

Use Python, Java, PostgreSQL, and a notebook editor such as VS Code or Jupyter. The local workflow has been exercised with Python 3.14, Java 17, and the pinned PySpark 4.2.0 dependency. Python 3.12 is the notebook's recommended starting point.

From the project root, create and activate an environment, then install the dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

These commands isolate the project's packages and install the versions used by the code. If you already use `venv`, activate that environment instead of creating another.

Check that Java is available:

```bash
java -version
```

Place **`postgresql-42.7.13.jar`** in the project root. The current Spark startup code checks for this filename, including when running Topic 3. Obtain the driver from the [PostgreSQL JDBC download page](https://jdbc.postgresql.org/download/).

For notebook execution, select your project environment as the kernel and open `capstone.ipynb` from the project root. If notebook support is missing from the environment:

```bash
python -m pip install jupyter ipykernel
```

## Prepare the source data

Run the notebook's **Generate the synthetic source extracts** section if the five files are not already present. The generator uses a fixed seed; rerunning it replaces the raw CSVs.

| Source | Main identifier | Contents |
|---|---|---|
| `properties.csv` | `property_id` | Property name and city. |
| `units.csv` | `unit_id` | Property relationship, floor area, and active status. |
| `leases.csv` | `lease_id` | Unit relationship, lease dates, and monthly rent. |
| `work_orders.csv` | `work_order_id` | Maintenance dates, priority, response time, cost, and status. |
| `meter_readings.csv` | `reading_id` | Unit, reading month, and energy consumption. |

The processing stages preserve raw data. Intentional defects are handled through quarantine rather than by editing the source files.

## Run the local pipeline

Run the notebook topic cells in order, or use these commands from the project root:

```bash
python -m src.ingest
python -m src.quality
python -m src.transform
python -m src.load_postgres
```

| Command | What it does |
|---|---|
| `src.ingest` | Reads the five CSVs and records their metadata. |
| `src.quality` | Runs ingestion, separates accepted and rejected rows, and saves quality counts. |
| `src.transform` | Reads staging data and writes the 72-row curated dataset. |
| `src.load_postgres` | Loads and verifies the database, then exports the SQL answers. |

Configure PostgreSQL as described below before running the last command. When inputs change, rerun the stages in order.

## Topic 1 — Python ingestion and reproducibility

The loader checks that each source exists, contains the required columns, and has data rows. It initially reads values as text to preserve identifiers such as leading-zero IDs. Number and date conversion happens in Topic 2.

`data/curated/ingestion_manifest.json` records the filename, row count, UTC ingestion timestamp, and SHA-256 file hash. The hash identifies the source file's contents.

A successful rerun replaces the manifest. Unchanged sources retain their hashes and row counts; timestamps change. All five sources must pass before a new manifest is saved. The module prevents writing the manifest into the raw directory.

## Topic 2 — Pandas validation, cleaning, and quarantine

Validation checks parent records before child records so that accepted rows do not reference rejected properties or units.

| Rule | Treatment |
|---|---|
| Exact duplicate | Keep the first copy; quarantine additional copies. |
| Conflicting duplicate identifier | Quarantine every conflicting row after removing exact copies. |
| Missing required values or invalid numbers/dates | Quarantine with a reason code. |
| Unknown property or unit | Quarantine the child record. |
| Negative amounts or energy; zero/negative floor area | Quarantine. |
| Invalid active flag, priority, or work-order status | Quarantine. |
| Invalid date order or inconsistent open/closed status | Quarantine. |
| Overlapping leases for one unit | Quarantine all otherwise-valid leases involved in the overlap. |
| Repeated unit and calendar month in meter readings | Quarantine competing readings after removing exact copies. |
| Meter readings outside January–June 2026 | Quarantine. |
| Missing response hours | Retain, flag as missing, and mark SLA compliance false. |

Accepted data is saved under `data/staging/`. Rejected rows retain their original values under `data/quarantine/`, with `reason_code` and `run_id`. The quality report is saved to `data/curated/quality_summary.json`.

For each source:

```text
input rows = accepted rows + quarantined rows
```

A row may have multiple reasons but is counted once in the quarantined total. Reruns replace the current outputs.

## Topic 3 — PySpark curated transformations

Spark reads staging CSVs using explicit schemas, builds the six-month calendar, and creates active-unit month-end lease snapshots. Occupancy, rent, maintenance, and energy are aggregated separately before joining them. This prevents multiple work orders from multiplying lease rent.

| KPI | Definition |
|---|---|
| Occupancy rate | Occupied active units at month end / total active units. |
| Earned monthly rent | Contractual rent on leases active at month end; this is not cash collected. |
| SLA compliance rate | Compliant resolved orders / all resolved orders. |
| Energy consumption | Sum of accepted monthly readings in kWh. |
| Energy intensity | Energy kWh / all accepted unit floor area, including inactive units. |

SLA means **Service Level Agreement**. Response limits are 4 hours for CRITICAL, 12 for HIGH, 48 for MEDIUM, and 72 for LOW. Resolved orders with missing responses remain in the denominator and count as noncompliant. Open orders are reported separately. Maintenance is assigned to the month it was opened.

Lease end dates are inclusive. Missing lease ends are open-ended. Zero or missing denominators produce null rates; ratios such as `0.5` mean 50%. Missing readings produce zero recorded energy, which does not establish actual zero consumption.

Spark validates 72 unique property-month rows and rates within 0–1, then overwrites:

```text
data/curated/property_month_kpis/month=2026-01-01/
...
data/curated/property_month_kpis/month=2026-06-01/
```

## Topic 4 — PostgreSQL dimensional mart and analytics

### Configure the connection

Start PostgreSQL. The loader and Topic 4 notebook now create the configured database automatically if it is missing, using the existing `postgres` maintenance database. Set `DB_NAME=proptech360` as shown below. The database user must be allowed to connect to `postgres` and have `CREATEDB` permission for first-time creation. Existing databases are left unchanged. `sql/00_database.sql` remains an optional manual alternative.

Create a project-root `.env` file with your server connection details:

```dotenv
DB_NAME=proptech360
DB_USER=postgres
DB_PASSWORD=your_postgresql_user_password
DB_HOST=localhost
DB_PORT=5432
```

Use the PostgreSQL user's password, not pgAdmin's master password. Keep `.env` private; it is ignored by Git. The loader also supports `DATABASE_URL`. A configured URL takes priority over separate fields, and terminal environment variables override matching `.env` settings.

### Load and query

```bash
python -m src.load_postgres
```

The current loader uses **psycopg2 batch inserts** for writes and the **PostgreSQL Java driver through Spark JDBC** for reads and verification.

| Database object | Purpose |
|---|---|
| `mart.dim_property` | Property identifier, name, and city. |
| `mart.dim_month` | One date for the first day of each reporting month. |
| `mart.fact_property_month` | Monthly counts, rent, energy, area, and calculated rates. |
| `mart.portfolio_dashboard` | Monthly portfolio totals and weighted rates. |

The fact primary key is `(property_id, month)`, with foreign keys to the dimensions. Check constraints reject invalid values. PostgreSQL generates rates from the stored counts and amounts.

The loader stages data in a temporary table and uses `ON CONFLICT ... DO UPDATE`. Repeated loads update existing keys and retain 72 rows. It does not delete obsolete keys; the fixed-period row-count check catches unexpected totals. Load failures roll back the transaction.

The five analytics queries cover occupancy ranking, rent by city, SLA performance, energy intensity above the portfolio benchmark, and the monthly dashboard. See `sql/03_analytics.sql`.

Current exports go into `evidence/04_postgres/` as `constraints.csv` and `query_1.csv` through `query_5.csv`. Existing `.txt` files are previously captured results. The loader also compares mart metrics with the curated Parquet data.

## Topic 5 — AWS serverless lake and Athena

The project's bucket is `proptech360-bucket-800557027629`. Use your own unique bucket name when reproducing this project in another account. Keep S3, Lambda, Glue, and Athena in the same AWS Region; the project console examples use Stockholm (`eu-north-1`).

### 1. Upload the datasets

Keep the bucket private and enable default encryption. Use these prefixes:

| S3 prefix | Contents |
|---|---|
| `raw/` | Source CSV uploads that trigger Lambda. |
| `curated/property_month_kpis/` | Actual Spark Parquet output, including all `month=...` folders. |
| `audit/` | JSON metadata written by Lambda. |
| `athena-result/` | Athena SELECT query results, normally CSV plus metadata. |

Upload the local curated directory while preserving its six month folders. Uploading only the Parquet files without their partition folders loses the month values stored in the paths.

### 2. Deploy Lambda and configure the trigger

Deploy `aws/lambda_handler.py` to Lambda and set the runtime handler to `lambda_handler.lambda_handler`. Alternatively, paste its contents into the console file `lambda_function.py` and use `lambda_function.lambda_handler`. The file matches the code inside the notebook's `LAMBDA_HANDLER_STARTER` string.

The Lambda execution role must trust `lambda.amazonaws.com` and allow:

- `s3:GetObject` and `s3:GetObjectVersion` on this bucket's `raw/*` objects.
- `s3:PutObject` on this bucket's `audit/*` objects.
- CloudWatch log group/stream creation and log writes for the function.

Use the S3 trigger with **All object create events**, prefix `raw/`, and suffix `.csv`. S3 must also have permission to invoke the function. Restricting the trigger to `raw/` prevents audit writes from triggering a loop.

The handler checks event type, path, extension, object size, and metadata. It creates a deterministic audit filename from the source bucket, key, and version or ETag. Repeated notifications overwrite the same audit object. Failed S3 operations raise errors so Lambda can retry. It records file metadata, not a row-by-row CSV validation.

Leave `AUDIT_BUCKET` unset to write audits to the source bucket. A different audit bucket requires matching write permissions. The supplied handler writes audits with S3-managed AES256 encryption.

Use this saved Lambda test event after uploading a nonempty `raw/properties.csv`:

```json
{
  "Records": [{
    "eventSource": "aws:s3",
    "eventName": "ObjectCreated:Put",
    "s3": {
      "bucket": {"name": "proptech360-bucket-800557027629"},
      "object": {"key": "raw/properties.csv"}
    }
  }]
}
```

Expect `files_audited: 1`, an audit JSON containing source details and `status: accepted`, and a corresponding CloudWatch log entry. Upload another CSV after configuring the trigger to verify automatic invocation.

### 3. Register the curated table

Configure a Glue crawler with this include path:

```text
s3://proptech360-bucket-800557027629/curated/property_month_kpis/
```

Use a Glue service role with permission to list/read that curated location and update the intended Data Catalog database. The Lambda execution role serves a different purpose.

Run the crawler and verify the table's Parquet schema and six month partitions. If creating the table manually, use these fields:

| Field | Glue type |
|---|---|
| `property_id` | string |
| `total_units` | bigint |
| `occupied_units` | bigint |
| `earned_monthly_rent_usd` | double |
| `resolved_orders` | bigint |
| `sla_compliant_orders` | bigint |
| `missing_response_orders` | bigint |
| `open_orders` | bigint |
| `sla_eligible_orders` | bigint |
| `sla_noncompliant_orders` | bigint |
| `energy_kwh` | double |
| `floor_area_sqm` | double |
| `occupancy_rate` | double |
| `sla_compliance_rate` | double |
| `energy_intensity_kwh_sqm` | double |

Add `month` as a **string partition key**, not another regular column. Set the table property `classification` to `parquet`. Parquet does not need `skip.header.line.count`.

### 4. Run Athena and reconcile results

Set the Athena query result location to:

```text
s3://proptech360-bucket-800557027629/athena-result/
```

The identity running Athena needs access to the workgroup, Glue catalog metadata, curated source files, and the query result location. Select the Glue database. The following queries assume the table is named `property_month_kpis`; adjust it if the crawler created a different name.

For a manually created partitioned table, register the month folders:

```sql
MSCK REPAIR TABLE property_month_kpis;
```

Find the top five property-month occupancy values:

```sql
SELECT property_id, month, total_units, occupied_units, occupancy_rate
FROM property_month_kpis
WHERE occupancy_rate IS NOT NULL
ORDER BY occupancy_rate DESC, property_id, month
LIMIT 5;
```

Calculate portfolio energy by month:

```sql
SELECT month, ROUND(SUM(energy_kwh), 2) AS total_energy_kwh
FROM property_month_kpis
GROUP BY month
ORDER BY month;
```

Run the matching query in pgAdmin:

```sql
SELECT TO_CHAR(month, 'YYYY-MM-DD') AS month,
       ROUND(CAST(SUM(energy_kwh) AS numeric), 2) AS total_energy_kwh
FROM mart.fact_property_month
GROUP BY month
ORDER BY month;
```

Compare all six monthly rows, not just the grand total. After rounding to two decimals, corresponding totals should match. Record any difference and investigate before claiming reconciliation.

If Athena returns one row with a blank month, check that `month` is a partition key, the files retain their month folders, and the table points at the dataset root. If only one month appears, check that all six partitions were uploaded and registered.

### 5. Save evidence and clean up

Save new evidence in the matching subfolder under `evidence/` (see the evidence index):

- CloudWatch Lambda logs showing the accepted source file.
- An example audit JSON and the Lambda test result.
- Athena occupancy and monthly energy query screenshots or CSV exports.
- Matching PostgreSQL monthly energy results and the comparison outcome.
- Glue schema and partition evidence.

Available AWS screenshots are organized by service under `evidence/05_aws/`. Duplicate screenshot folders have been removed from `aws/`, which now contains the Lambda handler. See [the evidence index](evidence/README.md) for file sources, the local monthly comparison, and outstanding captures. Existing Athena screenshots have blank month values, so they do not yet establish monthly AWS reconciliation.

S3 storage/requests, Lambda execution, CloudWatch logs, Glue crawler runs, and Athena scans can incur charges. Use small datasets, on-demand crawler runs, and Parquet partitions. Keep credentials out of code, notebook outputs, and screenshots.

When the lab is finished, disable the S3 notification, remove the project Lambda and crawler, remove unneeded catalog tables/databases, and delete project S3 objects and logs only when evidence is retained. Include object versions if bucket versioning is enabled. Remove the project bucket and IAM roles/policies only after confirming they are not shared.

## Tests and expected results

Run the complete suite from the project root:

```bash
python -m unittest discover -s tests -v
```

| Test file | Coverage |
|---|---|
| `test_ingest.py` | File/column checks, text preservation, manifests, and reruns. |
| `test_quality.py` | Duplicates, invalid values, relationships, dates, quarantine, and reconciliation. |
| `test_transform.py` | Occupancy, rent, SLA, energy, invalid keys, and Parquet reruns. |
| `test_database_config.py` | Private settings, passwords, URL support, and configuration errors. |
| `test_postgres.py` | Real database loads, constraints, counts, and Spark reconciliation. |

The last verified full run passed **39 tests**, including Spark and PostgreSQL. Database tests load the configured mart and require staging data, curated Parquet, Java, the JDBC driver, and a reachable PostgreSQL instance. Use a test database. Without database settings, these integration tests skip. AWS deployment is checked separately through the evidence workflow above.

Expected checks are accepted plus quarantined equals input, 72 unique curated keys, six month partitions, 72 fact rows after repeated loads, and matching monthly energy totals across systems.


The project is a small, fixed-period capstone. It uses current property attributes, does not maintain dimension history, and requires a separate curated upload to AWS. These boundaries keep the pipeline understandable and its results straightforward to verify.

## Run the pipeline with Airflow

The DAG in `dags/dags_script.py` keeps three sequential Python tasks:

```text
extraction_layer → transformation_layer → loading_layer
```

Extraction runs ingestion and Pandas validation. Transformation writes Spark Parquet. Loading upserts PostgreSQL, checks the results, and exports the analytics. Each Spark task stops its session even if it fails. Only one DAG run executes at a time because runs share output folders.

From the project root, activate your existing environment and set:

```bash
export AIRFLOW_HOME="$PWD/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
airflow standalone
```

`AIRFLOW_HOME` keeps runtime files inside the project; `DAGS_FOLDER` points to the actual DAG file's directory. Runtime files are ignored by Git. Start PostgreSQL and generate the raw CSVs first; Spark also needs Java and the PostgreSQL JAR described above. In the Airflow UI, enable `proptech360_dag` and trigger it manually. Restart running Airflow services after changing these environment settings.

The DAG retries failed tasks once after one minute. Email notifications are enabled only when `AIRFLOW_ALERT_EMAIL` is set; configure the `smtp_default` connection before enabling alerts. The DAG does not upload files to AWS or deploy Lambda.

## CI/CD with GitHub Actions

`.github/workflows/airflow.yml` tests pull requests and pushes to `main`. After tests pass on `main`, a self-hosted runner on your Mac updates the local checkout, validates the DAG, and triggers `proptech360_dag`. Manual runs are also available through **Actions → Test and run PropTech360 → Run workflow** on `main`.

CI runs the project tests with Python 3.14 and Java 17. PostgreSQL integration tests skip on GitHub because private database settings are not supplied. Deployment runs the real pipeline locally, including the loader's PostgreSQL reconciliation. GitHub waits up to 15 minutes for the DAG outcome and reports failed or timed-out runs as workflow failures.

### One-time activation

1. Commit and push the workflow and scripts to `main`.
2. In GitHub, open **Settings → Actions → Runners → New self-hosted runner**. Select **macOS / ARM64** for this Apple Silicon Mac. Follow the generated download/configuration commands in a separate folder outside this project. Add the custom label **`proptech360`** during configuration. Start it with the supplied `./run.sh` command.
3. Under **Settings → Secrets and variables → Actions → Variables**, create **`PROPTECH_PROJECT_DIR`** with the full project path:

   ```text
   /Users/sulaimon/Desktop/AMDARI-Data-Engineering/ClassNote/proptech360-capstone
   ```

4. Create the **`local-airflow`** environment under **Settings → Environments** and restrict deployment branches to `main`. This workflow only sends main-branch deployment jobs to the Mac; pull-request tests run on GitHub-hosted runners. A self-hosted runner executes repository code with your local user's permissions, so use it only for trusted code and restrict who can change workflows or push to `main`.
5. Keep the local checkout on `main` with no uncommitted changes. The runner must be a separate checkout from `PROPTECH_PROJECT_DIR`. The deployment directory needs the existing `venv`, `.env`, Java, JDBC JAR, and raw CSVs. Keep database secrets in the local `.env`; do not upload them to GitHub.
6. Start PostgreSQL and Airflow as shown in the previous section. Confirm `proptech360_dag` appears in Airflow before the first deployment. Leave the Mac awake, Airflow running, and the runner online.
7. Run the workflow from GitHub Actions, or push a change to `main`. Inspect the test job, deployment job, and Airflow task logs.

The deployment script stops if the checkout has local changes or an Airflow run is already queued/running. It uses a fast-forward merge of the tested commit; it never resets your checkout or overwrites `.env`. Pipeline runs can change tracked data/evidence outputs, so review and commit those changes before a later deployment. If Airflow's version changes, restart its services after updating dependencies before deploying. This workflow does not register a runner, start Airflow, install PostgreSQL or deploy AWS resources for you. The loading task creates the project database if it is missing and the configured user has permission.

Deployment requires a macOS ARM64 runner to match the existing ARM64 virtual environment. An X64 runner under Rosetta is not compatible. Replace an X64 runner using GitHub’s macOS ARM64 download; changing its labels alone does not change its architecture. The script checks architecture and the native Pydantic import before invoking Airflow.
