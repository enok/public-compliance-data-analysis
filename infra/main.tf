# 1. AWS Provider Configuration
provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "access_logs_bucket" {
  statement {
    sid    = "AllowS3ServerAccessLogging"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["logging.s3.amazonaws.com"]
    }

    actions = ["s3:PutObject"]

    resources = [
      "${aws_s3_bucket.access_logs.arn}/${var.access_log_prefix}*",
    ]

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.mba_datalake.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

data "aws_iam_policy_document" "kms_key" {
  #checkov:skip=CKV_AWS_356:A root-admin key policy necessarily targets the CMK itself with "*" resources.
  #checkov:skip=CKV_AWS_109:A minimal root-admin statement is required so the account can manage the CMK.
  #checkov:skip=CKV_AWS_111:A minimal root-admin statement is required so the account can manage the CMK.
  statement {
    sid    = "AllowAccountAdministration"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }

    actions   = ["kms:*"]
    resources = ["*"]
  }
}

resource "aws_kms_key" "s3" {
  description             = "Customer-managed KMS key for MBA thesis data lake buckets"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  policy                  = data.aws_iam_policy_document.kms_key.json

  tags = {
    Name        = "mba-thesis-s3-key"
    Environment = var.environment
    Project     = "MBA-Thesis-USP-Esalq"
    Owner       = "Enok Jesus"
  }
}

resource "aws_kms_alias" "s3" {
  name          = "alias/${var.environment}/mba-thesis-s3"
  target_key_id = aws_kms_key.s3.key_id
}

#tfsec:ignore:aws-s3-enable-bucket-logging
#checkov:skip=CKV_AWS_18:This bucket stores server access logs and should not recursively log to itself.
#checkov:skip=CKV2_AWS_62:Event notifications are not required for the dedicated access-log sink bucket.
#checkov:skip=CKV_AWS_144:Cross-region replication is intentionally out of scope for this thesis log sink.
resource "aws_s3_bucket" "access_logs" {
  #checkov:skip=CKV_AWS_18:This bucket stores server access logs and should not recursively log to itself.
  #checkov:skip=CKV2_AWS_62:Event notifications are not required for the dedicated access-log sink bucket.
  #checkov:skip=CKV_AWS_144:Cross-region replication is intentionally out of scope for this thesis log sink.
  bucket        = "${var.bucket_name}-access-logs"
  force_destroy = true

  tags = {
    Name        = "Public Compliance Access Logs"
    Environment = var.environment
    Project     = "MBA-Thesis-USP-Esalq"
    Owner       = "Enok Jesus"
  }
}

resource "aws_s3_bucket_versioning" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3.arn
      sse_algorithm     = "aws:kms"
    }

    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id

  rule {
    id     = "expire-access-logs"
    status = "Enabled"

    expiration {
      days = var.access_log_retention_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_bucket_policy" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id
  policy = data.aws_iam_policy_document.access_logs_bucket.json
}

#checkov:skip=CKV2_AWS_62:Event notifications are not required for this batch-oriented thesis data lake.
#checkov:skip=CKV_AWS_144:Cross-region replication is intentionally out of scope for this single-environment foundation.
resource "aws_s3_bucket" "mba_datalake" {
  #checkov:skip=CKV2_AWS_62:Event notifications are not required for this batch-oriented thesis data lake.
  #checkov:skip=CKV_AWS_144:Cross-region replication is intentionally out of scope for this single-environment foundation.
  bucket        = var.bucket_name
  force_destroy = true # Essential for automated teardown of non-empty buckets

  tags = {
    Name        = "Public Compliance Data Lake"
    Environment = var.environment
    Project     = "MBA-Thesis-USP-Esalq"
    Owner       = "Enok Jesus"
  }
}

resource "aws_s3_bucket_versioning" "mba_datalake" {
  bucket = aws_s3_bucket.mba_datalake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "mba_datalake" {
  bucket = aws_s3_bucket.mba_datalake.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3.arn
      sse_algorithm     = "aws:kms"
    }

    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "mba_datalake" {
  bucket = aws_s3_bucket.mba_datalake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_logging" "mba_datalake" {
  bucket = aws_s3_bucket.mba_datalake.id

  target_bucket = aws_s3_bucket.access_logs.id
  target_prefix = var.access_log_prefix
}

resource "aws_s3_bucket_lifecycle_configuration" "mba_datalake" {
  bucket = aws_s3_bucket.mba_datalake.id

  rule {
    id     = "protect-current-and-noncurrent-objects"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# 2. Medallion Layer Folders (S3 Prefixes)
resource "aws_s3_object" "layers" {
  for_each = toset(["bronze/", "silver/", "gold/"])

  bucket  = aws_s3_bucket.mba_datalake.id
  key     = each.value
  content = ""

  server_side_encryption = "aws:kms"
  kms_key_id             = aws_kms_key.s3.arn
}
