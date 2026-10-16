output "bucket_name" {
  description = "Upload raw CSVs and curated Parquet to this bucket."
  value       = aws_s3_bucket.lake.id
}
output "lambda_function_name" {
  value = aws_lambda_function.audit.function_name
}
output "glue_database" {
  value = aws_glue_catalog_database.analytics.name
}
output "glue_crawler" {
  value = aws_glue_crawler.kpis.name
}
output "athena_workgroup" {
  value = aws_athena_workgroup.analytics.name
}
