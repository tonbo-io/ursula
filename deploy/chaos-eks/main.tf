locals {
  service_account = "ursula-chaos-agent"
  role_name       = "${var.cluster_name}-ursula-chaos-agent"
  tags = merge(var.tags, {
    Project   = "ursula"
    Component = "chaos-agent"
    ManagedBy = "opentofu"
  })
}

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

resource "aws_iam_role" "chaos_agent" {
  name               = local.role_name
  assume_role_policy = data.aws_iam_policy_document.pod_identity_assume.json
  tags               = local.tags
}

data "aws_iam_policy_document" "status" {
  statement {
    sid       = "ReadBucketLocation"
    effect    = "Allow"
    actions   = ["s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::${var.status_bucket}"]
  }

  statement {
    sid       = "PublishChaosStatus"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["arn:aws:s3:::${var.status_bucket}/${var.status_key}"]
  }
}

resource "aws_iam_role_policy" "status" {
  name   = "ursula-chaos-status"
  role   = aws_iam_role.chaos_agent.id
  policy = data.aws_iam_policy_document.status.json
}

resource "aws_eks_pod_identity_association" "chaos_agent" {
  cluster_name    = var.cluster_name
  namespace       = var.namespace
  service_account = local.service_account
  role_arn        = aws_iam_role.chaos_agent.arn
}

resource "local_file" "helm_values" {
  filename        = "${path.module}/generated-values.yaml"
  file_permission = "0644"
  content = yamlencode({
    image = {
      repository = var.image_repository
      tag        = var.image_tag
    }
    serviceAccount = {
      create = true
      name   = local.service_account
    }
    statusS3Uri = "s3://${var.status_bucket}/${var.status_key}"
    target = {
      namespace          = var.namespace
      serverFullname     = var.server_fullname
      headlessService    = var.headless_service
      replicaCount       = 3
      indexerUrl         = "http://${var.indexer_service}:4493"
      indexSourceBaseUrl = "http://${var.server_fullname}:4437"
    }
  })
}
