variable "aws_region" {
  description = "AWS region for every resource"
  type        = string
  default     = "ap-south-1"
}

variable "project_name" {
  description = "Prefix for resource names"
  type        = string
  default     = "olist-pipeline"
}

variable "alert_email" {
  description = "Email that receives the cost alert"
  type        = string
}

variable "monthly_budget_usd" {
  description = "Cost alert fires at 80% of this amount"
  type        = number
  default     = 10
}

variable "max_bad_ratio" {
  description = "The Glue job refuses to publish if more than this share of a table is quarantined"
  type        = number
  default     = 0.05
}

variable "enable_redshift" {
  description = "Redshift Serverless is the main cost driver. Keep false until you reach the warehouse step."
  type        = bool
  default     = false
}

variable "redshift_admin_username" {
  type    = string
  default = "awsadmin"
}

variable "redshift_admin_password" {
  description = "Pass with TF_VAR_redshift_admin_password. Never commit it."
  type        = string
  default     = null
  sensitive   = true
}

variable "redshift_base_capacity" {
  description = "Redshift Serverless base capacity in RPUs (lowest allowed is cheapest)"
  type        = number
  default     = 8
}
