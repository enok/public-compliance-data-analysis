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