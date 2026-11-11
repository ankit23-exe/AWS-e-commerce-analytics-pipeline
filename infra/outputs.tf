output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "glue_job_name" {
  value = aws_glue_job.clean_transform.name
}

output "glue_database" {
  value = aws_glue_catalog_database.lake.name
}

output "curated_crawler_name" {
  value = aws_glue_crawler.curated.name
}

output "raw_crawler_name" {
  value = aws_glue_crawler.raw.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.main.name
}

output "redshift_role_arn" {
  description = "Use this as <REDSHIFT_ROLE_ARN> in sql/redshift/02_load_staging.sql"
  value       = var.enable_redshift ? aws_iam_role.redshift[0].arn : null
}
