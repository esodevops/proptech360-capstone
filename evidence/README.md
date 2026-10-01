# Evidence index

Evidence is grouped by pipeline topic, with a separate folder for reconciliation. Topic 3 has no standalone capture yet; its local Parquet totals are included in the reconciliation CSV. AWS screenshots are preserved unchanged here; duplicate screenshot folders were removed from `aws/`. Paths in the original-source column record their former locations. Captures describe earlier runs, not a fresh verification of the live AWS deployment.

## Folder order

```text
01_ingestion/       Ingestion manifest
02_quality/         Validation and quarantine summary
04_postgres/        Database constraints and five SQL results
05_aws/
  01_iam/           Role and test event
  02_s3/            Bucket and curated objects
  03_lambda/        Function and CloudWatch logs
  04_audit/         Audit object and content
  05_glue/          Database and table schema
  06_athena/        Queries and results
06_reconciliation/  Local Parquet versus saved PostgreSQL monthly totals
```

## Available evidence

| File | Purpose / original source |
|---|---|
| [constraints.txt](04_postgres/constraints.txt) | Saved PostgreSQL constraint results. |
| [query_1.txt](04_postgres/query_1.txt) | Saved PostgreSQL analytics query 1. |
| [query_2.txt](04_postgres/query_2.txt) | Saved PostgreSQL analytics query 2. |
| [query_3.txt](04_postgres/query_3.txt) | Saved PostgreSQL analytics query 3. |
| [query_4.txt](04_postgres/query_4.txt) | Saved PostgreSQL analytics query 4. |
| [query_5.txt](04_postgres/query_5.txt) | Saved PostgreSQL analytics query 5. |
| [ingestion_manifest.json](01_ingestion/ingestion_manifest.json) | Copy of the existing ingestion metadata and source hashes. |
| [quality_summary.json](02_quality/quality_summary.json) | Copy of the existing validation and quarantine counts. |
| [local_postgres_monthly_energy_comparison.csv](06_reconciliation/local_postgres_monthly_energy_comparison.csv) | Current local Parquet compared with saved PostgreSQL results in query_5.txt; all six monthly totals match to two decimal places. This is not a fresh database query or an Athena comparison. |
| [aws_athena_athena-result.png](05_aws/06_athena/athena-result.png) | `aws/athena/athena-result.png` |
| [aws_athena_athena-sql-1.png](05_aws/06_athena/athena-sql-1.png) | `aws/athena/athena-sql-1.png` |
| [aws_athena_athena-sql-1b.png](05_aws/06_athena/athena-sql-1b.png) | `aws/athena/athena-sql-1b.png` |
| [aws_athena_athena-sql-2a.png](05_aws/06_athena/athena-sql-2a.png) | `aws/athena/athena-sql-2a.png` |
| [aws_athena_athena-sql-2b-vs-pgAdmin.png](05_aws/06_athena/athena-sql-2b-vs-pgAdmin.png) | `aws/athena/athena-sql-2b-vs-pgAdmin.png` |
| [aws_athena_athena_vs_pgAdmin_sql.png](05_aws/06_athena/athena_vs_pgAdmin_sql.png) | `aws/athena/athena_vs_pgAdmin_sql.png` |
| [aws_audit_audit-a.png](05_aws/04_audit/audit-a.png) | `aws/audit/audit-a.png` |
| [aws_audit_audit-b.png](05_aws/04_audit/audit-b.png) | `aws/audit/audit-b.png` |
| [aws_aws_glue_proptech-database.png](05_aws/05_glue/proptech-database.png) | `aws/aws-glue/proptech-database.png` |
| [aws_aws_glue_proptech-table-2.png](05_aws/05_glue/proptech-table-2.png) | `aws/aws-glue/proptech-table-2.png` |
| [aws_aws_glue_proptech_table-1.png](05_aws/05_glue/proptech_table-1.png) | `aws/aws-glue/proptech_table-1.png` |
| [aws_lambda_CloudWtch.png](05_aws/03_lambda/CloudWtch.png) | `aws/lambda/CloudWtch.png` |
| [aws_lambda_lambda_function.png](05_aws/03_lambda/lambda_function.png) | `aws/lambda/lambda_function.png` |
| [aws_role_Event_JSON.png](05_aws/01_iam/Event_JSON.png) | `aws/role/Event_JSON.png` |
| [aws_role_role.png](05_aws/01_iam/role.png) | `aws/role/role.png` |
| [aws_s3_bucket_aws-bucket.png](05_aws/02_s3/aws-bucket.png) | `aws/s3-bucket/aws-bucket.png` |
| [aws_s3_bucket_properties_month_kpis.png](05_aws/02_s3/properties_month_kpis.png) | `aws/s3-bucket/properties_month_kpis.png` |

## What still needs a fresh capture

- **Athena monthly results and reconciliation:** the saved energy result screenshot shows one total with a blank month. The top-five occupancy screenshot also has blank months. The Glue schema capture shows `month` as a regular column, not a partition key. Fix the partition configuration and capture six monthly results before claiming monthly Athena reconciliation. The saved Athena screenshots use the table name `properties`.
- **CloudWatch accepted-file entry:** the available log screenshot shows invocation START/END/REPORT entries, but no structured JSON entry with `status: accepted`. Capture that entry from a successful audit invocation.
- **Audit JSON download:** audit screenshots show the object and its accepted metadata. The original downloaded JSON file is not present locally; no replacement has been fabricated.
- **Successful Lambda test response:** the event configuration screenshot is available, but a saved response showing `files_audited: 1` is still needed.
- **Glue partitions:** capture all six registered month partitions after correcting the schema.
- **Athena CSV export:** screenshots are included, but the actual downloaded query CSV is not present locally.

The comparison CSV was generated from existing local artifacts while organizing this folder. No AWS deployment changes or database loads were performed.
