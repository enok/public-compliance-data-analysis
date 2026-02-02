# 1. AWS Provider Configuration
provider "aws" {
  region = "us-east-1"
  # Note: Terraform will automatically use your CLI credentials
}

# 2. The Main S3 Bucket (The Data Lake)
resource "aws_s3_bucket" "mba_datalake" {
  bucket        = var.bucket_name
  force_destroy = true # Essential for automated teardown of non-empty buckets

  tags = {
    Name        = "Public Compliance Data Lake"
    Environment = var.environment
    Project     = "MBA-Thesis-USP-Esalq"
    Owner       = "Enok Jesus"
  }
}

# 3. Medallion Layer Folders (S3 Prefixes)
resource "aws_s3_object" "layers" {
  for_each = toset(["bronze/", "silver/", "gold/"])
  bucket   = aws_s3_bucket.mba_datalake.id
  key      = each.value
  content  = "" # Creates a zero-byte object to act as a folder
}