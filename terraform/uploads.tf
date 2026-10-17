# Paths are relative to this Terraform directory, not your terminal directory.
locals {
  raw_directory     = "${path.module}/../data/raw"
  curated_directory = "${path.module}/../data/curated/property_month_kpis"
}

# Wait for the trigger before uploading so first-time uploads are audited too.
resource "aws_s3_object" "raw" {
  for_each = fileset(local.raw_directory, "*.csv")

  bucket                 = aws_s3_bucket.lake.id
  key                    = "raw/${each.value}"
  source                 = "${local.raw_directory}/${each.value}"
  source_hash            = filemd5("${local.raw_directory}/${each.value}")
  content_type           = "text/csv"
  server_side_encryption = "AES256"

  depends_on = [
    aws_s3_bucket_notification.raw,
    aws_s3_bucket_server_side_encryption_configuration.lake,
    aws_s3_bucket_public_access_block.lake,
  ]
}

# Preserve month=... folders; exclude Spark markers and local checksum files.
resource "aws_s3_object" "curated" {
  for_each = fileset(local.curated_directory, "**/*.parquet")

  bucket                 = aws_s3_bucket.lake.id
  key                    = "curated/property_month_kpis/${each.value}"
  source                 = "${local.curated_directory}/${each.value}"
  source_hash            = filemd5("${local.curated_directory}/${each.value}")
  content_type           = "application/octet-stream"
  server_side_encryption = "AES256"

  depends_on = [
    aws_s3_bucket_server_side_encryption_configuration.lake,
    aws_s3_bucket_public_access_block.lake,
  ]
}
