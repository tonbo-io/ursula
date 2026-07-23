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

  validation {
    condition = length(var.cluster_endpoint_public_access_cidrs) > 0 && alltrue([
      for cidr in var.cluster_endpoint_public_access_cidrs : can(cidrhost(cidr, 0)) && !contains(["0.0.0.0/0", "::/0"], cidr)
    ])
    error_message = "cluster_endpoint_public_access_cidrs must contain valid restricted CIDRs; world-open CIDRs are not allowed."
  }
}

variable "node_instance_types" {
  description = "Allowed instance types for each zonal managed node group."
  type        = list(string)
  default     = ["m6i.xlarge"]

  validation {
    condition     = length(var.node_instance_types) > 0
    error_message = "node_instance_types must contain at least one instance type."
  }
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

variable "s3_noncurrent_version_expiration_days" {
  description = "Days to retain noncurrent S3 object versions. Keep the 30-day default for recovery; disposable chaos clusters can use 1."
  type        = number
  default     = 30

  validation {
    condition     = var.s3_noncurrent_version_expiration_days >= 1
    error_message = "s3_noncurrent_version_expiration_days must be at least 1."
  }
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
  description = "Explicit immutable release or build tag used by the generated Helm values."
  type        = string

  validation {
    condition     = trimspace(var.image_tag) != "" && var.image_tag != "latest"
    error_message = "image_tag must be an explicit non-empty tag and must not be latest."
  }
}

variable "server_core_count" {
  description = "Ursula runtime cores per voter."
  type        = number
  default     = 4

  validation {
    condition     = var.server_core_count >= 1
    error_message = "server_core_count must be at least 1."
  }
}

variable "server_cpu_request" {
  description = "CPU request for each Ursula voter."
  type        = string
  default     = "2"
}

variable "server_memory_request" {
  description = "Memory request for each Ursula voter."
  type        = string
  default     = "4Gi"
}

variable "server_memory_limit" {
  description = "Memory limit for each Ursula voter."
  type        = string
  default     = "8Gi"
}

variable "raft_group_count" {
  description = "Number of independent Raft groups."
  type        = number
  default     = 256

  validation {
    condition     = var.raft_group_count >= 1
    error_message = "raft_group_count must be at least 1."
  }
}

variable "raft_init_membership_per_group" {
  description = "Initialize static Raft membership. Set false after the first successful cluster bootstrap."
  type        = bool
  default     = true
}

variable "cold_compaction_enabled" {
  description = "Enable same-stream cold chunk compaction after every voter runs a compatible Ursula image."
  type        = bool
  default     = false
}

variable "raft_volume_size" {
  description = "gp3 PVC size for each voter Raft log."
  type        = string
  default     = "100Gi"
}

variable "gateway_replicas" {
  description = "Number of stateless gateway replicas."
  type        = number
  default     = 3

  validation {
    condition     = var.gateway_replicas >= 2
    error_message = "gateway_replicas must be at least 2 for the production topology."
  }
}

variable "indexer_replicas" {
  description = "Number of dynamic event-time indexer workers."
  type        = number
  default     = 2

  validation {
    condition     = var.indexer_replicas >= 2
    error_message = "indexer_replicas must be at least 2 for the production topology."
  }
}

variable "tags" {
  description = "Additional tags applied to AWS resources."
  type        = map(string)
  default     = {}
}
