# Simple Terraform setup for PropTech 360

This folder creates the AWS part of Topic 5: a private encrypted S3 bucket, the existing audit Lambda, its S3 trigger, CloudWatch logs (seven-day retention), IAM service roles, a Glue database and crawler, and an Athena workgroup. It also uploads local raw CSVs and curated Parquet during `terraform apply`. Spark, PostgreSQL, and Airflow continue to run locally.

Everything is in plain resource blocks: no modules, workspaces, remote backend, or deployment scripts. Terraform stores its resource tracking in a local `terraform.tfstate` file. Keep that file private and backed up; do not delete it or commit it. Commit `.terraform.lock.hcl` so provider selections are reproducible.

| File | Purpose |
|---|---|
| `main.tf` | Providers and AWS resources, in dependency order. |
| `uploads.tf` | Upload local CSV and Parquet files, detecting content changes. |
| `variables.tf` | Region input, defaulting to Stockholm (`eu-north-1`). |
| `outputs.tf` | Names to use after creation. |
| `terraform.tfvars.example` | Optional example settings. |

The bucket gets a unique `proptech360-tf-` name. Other resource names use `proptech360-tf`; use this setup once per account/region. Existing manually created resources are not imported or changed. If AWS reports a name already exists, investigate that resource before changing names or importing it.

## 1. Check your tools and AWS identity

Install Terraform and AWS CLI if missing. From the project root:

```bash
terraform version
aws --version
aws configure
aws sts get-caller-identity
cd terraform
```

`aws configure` is only needed when you have no working AWS credentials. Existing SSO or environment-based credentials also work. For a named profile, run `export AWS_PROFILE=your-profile` before the commands. Check the account returned by `get-caller-identity`. Never put access keys in Terraform files.

Your deployment identity needs permission to manage S3, Lambda, CloudWatch Logs, IAM roles and policies (including `iam:PassRole`), Glue, and Athena. The created service roles do not grant your user access to run Athena queries. Your query identity also needs Athena workgroup access, Glue metadata access, read access to curated objects, and read/write access to the result prefix. Accounts using Lake Formation may require additional data permissions.

Optional: copy `terraform.tfvars.example` to `terraform.tfvars` and edit the region before creating resources. Use the same region for later AWS CLI commands.

## Prepare local datasets before planning

The five CSV files must exist under `data/raw/`. Generate curated Parquet with the local notebook or pipeline before running Terraform. From the project root:

```bash
python -m src.ingest
python -m src.quality
python -m src.transform
cd terraform
```

Terraform uploads existing files; it does not generate them. It discovers `data/raw/*.csv` and `data/curated/property_month_kpis/**/*.parquet` at plan time. An empty or missing directory contributes no upload resources. Keep files unchanged between plan and apply.

## 2. Initialize

```bash
terraform init
```

Downloads the AWS and archive providers. The archive provider packages the existing `aws/lambda/lambda_handler.py`; no manual ZIP step is required.

## 3. Format and validate

```bash
terraform fmt
terraform validate
```

`fmt` formats the configuration. `validate` checks its structure; it does not verify AWS permissions or create anything.

## 4. Preview

```bash
terraform plan
```

Review the resources marked `+` for creation. This configuration creates a new bucket; it does not reuse the bucket in the original README. AWS usage can incur charges, especially Glue crawler runs and Athena queries. The Athena workgroup limits each query to 1 GiB scanned; this is not a total spending cap.

## 5. Create

```bash
terraform apply
```

Read the displayed plan, then type `yes` to create the resources. Terraform handles dependency ordering, including Lambda invoke permission before the S3 notification. If an initial run fails, fix the reported cause and run `terraform plan` and `terraform apply` again; retain state so Terraform can track resources already created.

```bash
terraform output
terraform state list
terraform plan
```

The final plan should show no changes after a successful apply. `output` shows your new resource names; `state list` shows tracked resources.

## 6. Verify uploads and run the crawler

`terraform apply` now uploads all matching CSV and Parquet files automatically. It waits for the S3 trigger before uploading raw CSVs, so Lambda audits the initial upload. Parquet keys retain their month folders. File hashes detect changes for the next apply. No separate `aws s3 cp` step is needed. Terraform also creates an empty `athena-result/` folder marker so the configured query result location is visible in S3 before the first query. Athena writes actual result files there when queries run.

Run these verification and crawler commands from this `terraform/` directory:

```bash
export AWS_DEFAULT_REGION=eu-north-1
BUCKET=$(terraform output -raw bucket_name)
aws s3 ls "s3://$BUCKET/raw/"
aws s3 ls "s3://$BUCKET/curated/property_month_kpis/"
aws s3 ls "s3://$BUCKET/audit/"
aws glue start-crawler --name proptech360-tf-kpis
aws glue get-crawler --name proptech360-tf-kpis --query 'Crawler.{State:State,LastCrawl:LastCrawl}'
```

If you changed the region, change `AWS_DEFAULT_REGION` too. Audit objects may take a few seconds to appear. The upload keeps the `month=...` partition folders. After the crawler returns to `READY`, confirm `LastCrawl.Status` is `SUCCEEDED` and inspect its table:

```bash
aws glue get-tables --database-name proptech360_tf --query 'TableList[].{Name:Name,Partitions:PartitionKeys,Location:StorageDescriptor.Location}'
aws logs tail /aws/lambda/proptech360-tf-audit --since 10m
```

In the Athena console, select the same region, workgroup `proptech360-tf`, and database `proptech360_tf`. The crawler should create `property_month_kpis`; check the name and `month` partition key before querying:

```sql
SELECT month, COUNT(*) AS property_count,
       ROUND(SUM(energy_kwh), 2) AS total_energy_kwh
FROM property_month_kpis
GROUP BY month
ORDER BY month;
```

Expect six months with 12 properties per month. Compare monthly energy totals with PostgreSQL using the Topic 5 queries in the project README. The crawler registers partitions, so a separate `MSCK REPAIR TABLE` is normally unnecessary. Run `terraform plan` and `terraform apply` after generating new data, then rerun the crawler when new partitions are added. Terraform apply does not run the crawler or the local pipeline.

Files removed locally are planned for deletion from S3. Regenerating Spark output may replace filenames, so inspect both additions and deletions in the plan. Terraform manages only the source objects declared in `uploads.tf`; Lambda audit files and Athena results are generated separately. On first adoption of existing CLI uploads, Terraform writes those same keys and starts tracking them.

## 7. Make later infrastructure changes

Edit the `.tf` files, then use the same commands:

```bash
terraform fmt
terraform validate
terraform plan
terraform apply
```

Updating the Lambda source changes its ZIP hash, so Terraform deploys it on the next apply. Keep using the same directory and state. Avoid editing managed resources in the console, since the next apply may restore the Terraform settings.

## 8. Remove the lab when finished

Save evidence and needed data first. Stop running crawls and queries. Terraform deletes the managed uploaded objects during destroy. The bucket still refuses deletion while generated audit files, Athena results, or other unmanaged files remain. To permanently empty only this lab bucket, review its name before running the removal command:

```bash
terraform output -raw bucket_name
BUCKET=$(terraform output -raw bucket_name)
aws s3 rm "s3://$BUCKET/" --recursive
terraform plan -destroy
terraform destroy
```

Read the destruction plan and type `yes` only when ready. This removes the resources tracked by this configuration, including its log group and Glue database. It does not remove the original manually created project bucket or local data. Keep Terraform state until cleanup completes.

Provider reference: [S3 notifications](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_notification). A single notification resource owns the new bucket's notification configuration.

### Recovering an interrupted Athena cleanup

The Athena workgroup uses `force_destroy = true` so a workgroup containing queries can be removed during lab cleanup. If using older state after a failed destroy, persist this setting without recreating deleted infrastructure:

```bash
terraform plan -target=aws_athena_workgroup.analytics
terraform apply -target=aws_athena_workgroup.analytics
terraform destroy
```

Review the targeted plan: it should only change the workgroup cleanup setting. Targeting is for this recovery case, not the normal deployment workflow.
