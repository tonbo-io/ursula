variable "name" {
  description = "Name prefix for the EKS cluster and AWS resources."
  type        = string
  default     = "ursula"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,38}[a-z0-9]$", var.name))
    error_message = "name must be a lowercase DNS-style name between 3 and 40 characters."
  }
}

variable "aws_region" {
  description = "AWS region in which to create the cluster."
  type        = string
  default     = "us-east-1"
}

variable "availability_zones" {
  description = "Exactly three availability zones. Leave empty to use the first three available zones in aws_region."
  type        = list(string)
  default     = []

  validation {
    condition     = length(var.availability_zones) == 0 || length(var.availability_zones) == 3
    error_message = "availability_zones must be empty or contain exactly three zones."
  }
}

variable "vpc_cidr" {
  description = "CIDR allocated to the Ursula VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "single_nat_gateway" {
  description = "Use one NAT gateway instead of one per AZ. This saves cost but makes private-node egress depend on one AZ."
  type        = bool
  default     = false
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version."
  type        = string
  default     = "1.35"
}

variable "cluster_endpoint_public_access_cidrs" {
  description = "CIDRs allowed to reach the public EKS API endpoint. Restrict this to operator networks in production."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "node_instance_types" {
  description = "Allowed instance types for each zonal managed node group."
  type        = list(string)
  default     = ["m6i.xlarge"]
}

variable "nodes_per_az" {
  description = "Desired node count in each zonal managed node group."
  type        = number
  default     = 1

  validation {
    condition     = var.nodes_per_az >= 1
    error_message = "nodes_per_az must be at least 1."
  }
}

variable "max_nodes_per_az" {
  description = "Maximum node count in each zonal managed node group."
  type        = number
  default     = 3

  validation {
    condition     = var.max_nodes_per_az >= var.nodes_per_az
    error_message = "max_nodes_per_az must be greater than or equal to nodes_per_az."
  }
}

variable "s3_bucket_name" {
  description = "Optional globally unique bucket name. Empty generates name-account-region."
  type        = string
  default     = ""
}

variable "s3_force_destroy" {
  description = "Allow tofu destroy to delete a non-empty data bucket. Keep false for production."
  type        = bool
  default     = false
}

variable "namespace" {
  description = "Kubernetes namespace passed to Helm and EKS Pod Identity."
  type        = string
  default     = "ursula"
}

variable "release_name" {
  description = "Helm release name. The generated values pin resource names to this value."
  type        = string
  default     = "ursula"
}

variable "image_repository" {
  description = "Container image repository used by the generated Helm values."
  type        = string
  default     = "ghcr.io/tonbo-io/ursula"
}

variable "image_tag" {
  description = "Container image tag used by the generated Helm values."
  type        = string
  default     = "0.2.0"
}

variable "tags" {
  description = "Additional tags applied to AWS resources."
  type        = map(string)
  default     = {}
}
