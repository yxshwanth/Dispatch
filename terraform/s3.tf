resource "aws_s3_bucket" "payloads" {
  bucket = "${local.name}-${data.aws_caller_identity.current.account_id}-${var.aws_region}-payloads"
  tags   = local.tags
}

resource "aws_s3_bucket_public_access_block" "payloads" {
  bucket = aws_s3_bucket.payloads.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "payloads" {
  bucket = aws_s3_bucket.payloads.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "payloads" {
  bucket = aws_s3_bucket.payloads.id

  rule {
    id     = "glacier-after-90-days"
    status = "Enabled"

    filter {}

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# Versioning intentionally disabled for demo archival bucket (not required).
# Enable aws_s3_bucket_versioning if object recovery history is needed.
