resource "aws_s3_bucket" "ursula" {
  bucket        = local.bucket_name
  force_destroy = var.s3_force_destroy
}

resource "aws_s3_bucket_public_access_block" "ursula" {
  bucket = aws_s3_bucket.ursula.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "ursula" {
  bucket = aws_s3_bucket.ursula.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "ursula" {
  bucket = aws_s3_bucket.ursula.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "ursula" {
  bucket = aws_s3_bucket.ursula.id

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  depends_on = [aws_s3_bucket_versioning.ursula]
}
