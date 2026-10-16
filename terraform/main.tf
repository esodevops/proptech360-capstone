terraform {
  required_version = ">= 1.5, < 2.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = { Project = "proptech360", ManagedBy = "Terraform" }
  }
}

data "aws_caller_identity" "current" {}

# A new bucket avoids changing the existing manually created project bucket.
resource "aws_s3_bucket" "lake" {
  bucket_prefix = "proptech360-tf-"
  force_destroy = false
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_iam_role" "lambda" {
  name_prefix = "proptech360-tf-lambda-"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_cloudwatch_log_group" "audit" {
  name              = "/aws/lambda/proptech360-tf-audit"
  retention_in_days = 7
}

resource "aws_iam_role_policy" "lambda" {
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject", "s3:GetObjectVersion"], Resource = "${aws_s3_bucket.lake.arn}/raw/*" },
      { Effect = "Allow", Action = "s3:PutObject", Resource = "${aws_s3_bucket.lake.arn}/audit/*" },
      { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.audit.arn}:*" }
    ]
  })
}

data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/../aws/lambda/lambda_handler.py"
  output_path = "${path.module}/lambda.zip"
}

resource "aws_lambda_function" "audit" {
  function_name    = "proptech360-tf-audit"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "lambda_handler.lambda_handler"
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  timeout          = 30
  memory_size      = 128
  depends_on       = [aws_iam_role_policy.lambda]
}

resource "aws_lambda_permission" "s3" {
  statement_id   = "AllowProjectS3"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.audit.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.lake.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_s3_bucket_notification" "raw" {
  bucket = aws_s3_bucket.lake.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.audit.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
    filter_suffix       = ".csv"
  }
  depends_on = [aws_lambda_permission.s3]
}

resource "aws_glue_catalog_database" "analytics" {
  name = "proptech360_tf"
}

resource "aws_iam_role" "glue" {
  name_prefix = "proptech360-tf-glue-"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

# AWS's standard Glue service policy supplies catalog and logging permissions.
resource "aws_iam_role_policy_attachment" "glue" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = "s3:ListBucket", Resource = aws_s3_bucket.lake.arn, Condition = { StringLike = { "s3:prefix" = ["curated", "curated/*"] } } },
      { Effect = "Allow", Action = "s3:GetObject", Resource = "${aws_s3_bucket.lake.arn}/curated/*" }
    ]
  })
}

resource "aws_glue_crawler" "kpis" {
  name          = "proptech360-tf-kpis"
  database_name = aws_glue_catalog_database.analytics.name
  role          = aws_iam_role.glue.arn
  s3_target {
    path = "s3://${aws_s3_bucket.lake.id}/curated/property_month_kpis/"
  }
  depends_on = [aws_iam_role_policy_attachment.glue, aws_iam_role_policy.glue_s3]
}

resource "aws_athena_workgroup" "analytics" {
  name = "proptech360-tf"
  # Allow lab cleanup after queries have been run.
  force_destroy = true
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 1073741824 # 1 GiB per query
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.id}/athena-result/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
