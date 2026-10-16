# Deployment record

Created on 16 October 2026 using `terraform init`, `terraform fmt`, `terraform validate`, `terraform plan -out=deployment.tfplan`, and `terraform apply deployment.tfplan`.

- AWS account: `800557027629`
- Region: `eu-north-1`
- Result: 15 resources added, none changed or destroyed.
- Bucket: `proptech360-tf-4ecf1f228ba3caebb14cbe2c10`
- Lambda: `proptech360-tf-audit`
- Glue database: `proptech360_tf`
- Glue crawler: `proptech360-tf-kpis`
- Athena workgroup: `proptech360-tf`

The original resources were absent and local state was empty when completing the data setup. The infrastructure was recreated with a fresh Terraform plan and apply on 16 October 2026; the bucket above is the current bucket.

Data setup completed:
- Uploaded five raw CSV files and 72 Parquet files, preserving six month folders.
- S3 invoked Lambda automatically, generating five audit JSON objects.
- Glue crawler completed with SUCCEEDED and created property_month_kpis with a string month partition key.
- Athena verification query succeeded with six months and 12 properties per month (72 records total).
- Query ID: 8afe3cc0-65c7-42a7-a10f-09f27c57c610; bytes scanned: 2371.
- Evidence: evidence/05_aws/terraform/audit/ and evidence/05_aws/terraform/monthly_verification.csv.

PostgreSQL reconciliation was not rerun as part of this upload.

Local state is kept in this directory and ignored by Git. Keep it backed up privately for future updates and cleanup. The saved deployment plan describes the initial creation; run a fresh `terraform plan` for later changes.
