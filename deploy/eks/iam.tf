data "aws_iam_policy_document" "pod_identity_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole", "sts:TagSession"]

    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "server_irsa_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:sub"
      values   = ["system:serviceaccount:${var.namespace}:${local.server_sa}"]
    }
  }
}

data "aws_iam_policy_document" "indexer_irsa_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:sub"
      values   = ["system:serviceaccount:${var.namespace}:${local.indexer_sa}"]
    }
  }
}

resource "aws_iam_role" "server" {
  name               = "${local.cluster_name}-storage"
  assume_role_policy = data.aws_iam_policy_document.server_irsa_assume.json
}

resource "aws_iam_role" "indexer" {
  name               = "${local.cluster_name}-indexer"
  assume_role_policy = data.aws_iam_policy_document.indexer_irsa_assume.json
}

resource "aws_iam_role" "ebs_csi" {
  name               = "${local.cluster_name}-ebs-csi"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_assume.json
}

resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}

data "aws_iam_policy_document" "server_s3" {
  statement {
    sid       = "ListStoragePrefix"
    effect    = "Allow"
    actions   = ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"]
    resources = [aws_s3_bucket.ursula.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = [local.server_prefix, "${local.server_prefix}/*"]
    }
  }

  statement {
    sid       = "ManageStorageObjects"
    effect    = "Allow"
    actions   = ["s3:AbortMultipartUpload", "s3:DeleteObject", "s3:GetObject", "s3:ListMultipartUploadParts", "s3:PutObject"]
    resources = ["${aws_s3_bucket.ursula.arn}/${local.server_prefix}/*"]
  }
}

data "aws_iam_policy_document" "indexer_s3" {
  statement {
    sid       = "ListIndexPrefix"
    effect    = "Allow"
    actions   = ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"]
    resources = [aws_s3_bucket.ursula.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = [local.index_prefix, "${local.index_prefix}/*"]
    }
  }

  statement {
    sid       = "ManageIndexObjects"
    effect    = "Allow"
    actions   = ["s3:AbortMultipartUpload", "s3:DeleteObject", "s3:GetObject", "s3:ListMultipartUploadParts", "s3:PutObject"]
    resources = ["${aws_s3_bucket.ursula.arn}/${local.index_prefix}/*"]
  }
}

resource "aws_iam_role_policy" "server_s3" {
  name   = "ursula-storage"
  role   = aws_iam_role.server.id
  policy = data.aws_iam_policy_document.server_s3.json
}

resource "aws_iam_role_policy" "indexer_s3" {
  name   = "ursula-index"
  role   = aws_iam_role.indexer.id
  policy = data.aws_iam_policy_document.indexer_s3.json
}
