variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "Globally unique name for the S3 Data Lake"
  type        = string
  default     = "enok-mba-thesis-datalake"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "access_log_prefix" {
  description = "Prefix used for server access logs stored in the dedicated log bucket"
  type        = string
  default     = "s3-access-logs/"
}

variable "access_log_retention_days" {
  description = "Retention period for S3 server access logs"
  type        = number
  default     = 90
}
