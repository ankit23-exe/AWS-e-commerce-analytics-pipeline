# Optional: set enable_redshift = true when you reach the warehouse step.
# Redshift Serverless bills while queries run. Run `terraform destroy` (or set the flag back
# to false and apply) when you finish a session.

data "aws_vpc" "default" {
  count   = var.enable_redshift ? 1 : 0
  default = true
}

data "aws_subnets" "default" {
  count = var.enable_redshift ? 1 : 0

  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default[0].id]
  }
}

data "aws_iam_policy_document" "redshift_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift" {
  count              = var.enable_redshift ? 1 : 0
  name               = "${var.project_name}-redshift-role"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
}

data "aws_iam_policy_document" "redshift_s3" {
  statement {
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
  }
}

resource "aws_iam_role_policy" "redshift_s3" {
  count  = var.enable_redshift ? 1 : 0
  name   = "read-lake"
  role   = aws_iam_role.redshift[0].id
  policy = data.aws_iam_policy_document.redshift_s3.json
}

resource "aws_redshiftserverless_namespace" "this" {
  count                = var.enable_redshift ? 1 : 0
  namespace_name       = var.project_name
  db_name              = "dev"
  admin_username       = var.redshift_admin_username
  admin_user_password  = var.redshift_admin_password
  iam_roles            = [aws_iam_role.redshift[0].arn]
  default_iam_role_arn = aws_iam_role.redshift[0].arn
}

resource "aws_redshiftserverless_workgroup" "this" {
  count               = var.enable_redshift ? 1 : 0
  namespace_name      = aws_redshiftserverless_namespace.this[0].namespace_name
  workgroup_name      = var.project_name
  base_capacity       = var.redshift_base_capacity
  publicly_accessible = false
  subnet_ids          = data.aws_subnets.default[0].ids
}
