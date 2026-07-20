data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

locals {
  availability_zones = length(var.availability_zones) == 0 ? slice(data.aws_availability_zones.available.names, 0, 3) : var.availability_zones
  cluster_name       = "${var.name}-eks"
  bucket_name        = var.s3_bucket_name != "" ? var.s3_bucket_name : "${var.name}-${data.aws_caller_identity.current.account_id}-${var.aws_region}"
  private_subnets    = [for index in range(3) : cidrsubnet(var.vpc_cidr, 4, index)]
  public_subnets     = [for index in range(3) : cidrsubnet(var.vpc_cidr, 4, index + 8)]
  server_prefix      = "storage"
  index_prefix       = "indexes"
  server_sa          = "ursula-storage"
  indexer_sa         = "ursula-indexer"
  tags = merge(var.tags, {
    Project   = "ursula"
    ManagedBy = "opentofu"
  })
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "6.6.1"

  name = local.cluster_name
  cidr = var.vpc_cidr

  azs             = local.availability_zones
  private_subnets = local.private_subnets
  public_subnets  = local.public_subnets

  enable_nat_gateway     = true
  single_nat_gateway     = var.single_nat_gateway
  one_nat_gateway_per_az = !var.single_nat_gateway

  enable_dns_hostnames = true
  enable_dns_support   = true

  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
  }
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "21.9.0"

  name               = local.cluster_name
  kubernetes_version = var.kubernetes_version

  endpoint_private_access                  = true
  endpoint_public_access                   = true
  endpoint_public_access_cidrs             = var.cluster_endpoint_public_access_cidrs
  enable_cluster_creator_admin_permissions = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  addons = {
    coredns    = {}
    kube-proxy = {}
    vpc-cni = {
      before_compute = true
    }
    eks-pod-identity-agent = {
      before_compute = true
    }
    aws-ebs-csi-driver = {
      pod_identity_association = [{
        role_arn        = aws_iam_role.ebs_csi.arn
        service_account = "ebs-csi-controller-sa"
      }]
    }
  }

  eks_managed_node_groups = {
    for index, zone in local.availability_zones : "ursula-${replace(zone, var.aws_region, "")}" => {
      subnet_ids     = [module.vpc.private_subnets[index]]
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = var.node_instance_types
      capacity_type  = "ON_DEMAND"
      min_size       = var.nodes_per_az
      desired_size   = var.nodes_per_az
      max_size       = var.max_nodes_per_az

      update_config = {
        max_unavailable = 1
      }

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            encrypted             = true
            volume_size           = 50
            volume_type           = "gp3"
            delete_on_termination = true
          }
        }
      }

      labels = {
        "ursula.tonbo.io/node-pool" = "voter"
      }
    }
  }
}

resource "kubernetes_storage_class_v1" "gp3" {
  metadata {
    name = "gp3"
  }

  storage_provisioner    = "ebs.csi.aws.com"
  reclaim_policy         = "Retain"
  volume_binding_mode    = "WaitForFirstConsumer"
  allow_volume_expansion = true

  parameters = {
    type      = "gp3"
    encrypted = "true"
  }

  depends_on = [module.eks]
}
