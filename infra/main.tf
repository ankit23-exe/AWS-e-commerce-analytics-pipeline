data "aws_caller_identity" "current" {}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  lake_bucket = "${var.project_name}-${local.account_id}-lake"
  lake_uri    = "s3://${aws_s3_bucket.lake.bucket}"
  db_name     = replace("${var.project_name}_db", "-", "_")
  job_name    = "${var.project_name}-clean-transform"
}

# --------------------------------------------------------------------------- #
# Data lake bucket: raw/ (bronze), curated/ (silver), quarantine/, quality_reports/
# --------------------------------------------------------------------------- #
resource "aws_s3_bucket" "lake" {
  bucket        = local.lake_bucket
  force_destroy = true # learning project: `terraform destroy` also empties the bucket
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

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "expire-athena-results"
    status = "Enabled"

    filter {
      prefix = "athena-results/"
    }

    expiration {
      days = 7
    }
  }
}

# --------------------------------------------------------------------------- #
# IAM: one role per service, least privilege on the lake bucket
# --------------------------------------------------------------------------- #
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.project_name}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_lake" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }

  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
  }

  statement {
    actions   = ["glue:StartCrawler", "glue:GetCrawler"]
    resources = ["arn:aws:glue:${var.aws_region}:${local.account_id}:crawler/${var.project_name}-*"]
  }
}

resource "aws_iam_role_policy" "glue_lake" {
  name   = "lake-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_lake.json
}

# --------------------------------------------------------------------------- #
# Glue: catalog database, ETL job, crawlers
# --------------------------------------------------------------------------- #
resource "aws_glue_catalog_database" "lake" {
  name = local.db_name
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/clean_transform.py"
  source = "${path.module}/../src/glue/clean_transform.py"
  etag   = filemd5("${path.module}/../src/glue/clean_transform.py")
}

resource "aws_glue_job" "clean_transform" {
  name              = local.job_name
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30 # minutes
  max_retries       = 0

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--raw_path"                         = "${local.lake_uri}/raw"
    "--curated_path"                     = "${local.lake_uri}/curated"
    "--quarantine_path"                  = "${local.lake_uri}/quarantine"
    "--report_path"                      = "${local.lake_uri}/quality_reports"
    "--max_bad_ratio"                    = tostring(var.max_bad_ratio)
    "--curated_crawler_name"             = "${var.project_name}-curated"
  }
}

resource "aws_glue_crawler" "curated" {
  name          = "${var.project_name}-curated"
  database_name = aws_glue_catalog_database.lake.name
  role          = aws_iam_role.glue.arn
  table_prefix  = "curated_"

  s3_target {
    path = "${local.lake_uri}/curated/"
  }

  schema_change_policy {
    delete_behavior = "DEPRECATE_IN_DATABASE"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}

# Lets Athena query the raw CSVs, so you can compare data scanned: raw CSV vs curated Parquet
resource "aws_glue_crawler" "raw" {
  name          = "${var.project_name}-raw"
  database_name = aws_glue_catalog_database.lake.name
  role          = aws_iam_role.glue.arn
  table_prefix  = "raw_"

  s3_target {
    path = "${local.lake_uri}/raw/"
  }

  schema_change_policy {
    delete_behavior = "DEPRECATE_IN_DATABASE"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}

# --------------------------------------------------------------------------- #
# Athena workgroup with a per-query scan cap (cost guard)
# --------------------------------------------------------------------------- #
resource "aws_athena_workgroup" "main" {
  name          = var.project_name
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = 1073741824 # 1 GB

    result_configuration {
      output_location = "${local.lake_uri}/athena-results/"
    }
  }
}

# --------------------------------------------------------------------------- #
# Lambda: S3 "_READY" marker -> start the Glue job
# --------------------------------------------------------------------------- #
data "archive_file" "trigger_lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/lambda/trigger_glue.py"
  output_path = "${path.module}/.build/trigger_glue.zip"
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.project_name}-trigger-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda_glue" {
  statement {
    actions   = ["glue:StartJobRun"]
    resources = ["arn:aws:glue:${var.aws_region}:${local.account_id}:job/${local.job_name}"]
  }
}

resource "aws_iam_role_policy" "lambda_glue" {
  name   = "start-glue-job"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_glue.json
}

resource "aws_lambda_function" "trigger" {
  function_name    = "${var.project_name}-trigger-glue"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "trigger_glue.handler"
  filename         = data.archive_file.trigger_lambda.output_path
  source_code_hash = data.archive_file.trigger_lambda.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      GLUE_JOB_NAME = aws_glue_job.clean_transform.name
    }
  }
}

resource "aws_lambda_permission" "allow_s3" {
  statement_id   = "AllowS3Invoke"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.trigger.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.lake.arn
  source_account = local.account_id
}

resource "aws_s3_bucket_notification" "ready" {
  bucket = aws_s3_bucket.lake.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
    filter_suffix       = "_READY"
  }

  depends_on = [aws_lambda_permission.allow_s3]
}

# --------------------------------------------------------------------------- #
# Cost guard
# --------------------------------------------------------------------------- #
resource "aws_budgets_budget" "monthly" {
  name         = "${var.project_name}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
}
