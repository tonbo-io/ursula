variable "aws_region" {
  description = "AWS region containing the target EKS cluster and status bucket."
  type        = string
}

variable "cluster_name" {
  description = "Existing EKS cluster that runs Ursula."
  type        = string
}

variable "namespace" {
  description = "Namespace for both the Ursula and ursula-chaos releases."
  type        = string
  default     = "ursula"
}

variable "status_bucket" {
  description = "Existing S3 bucket that receives the public chaos status object."
  type        = string
}

variable "status_key" {
  description = "S3 object key used by the agent for status restore and publication."
  type        = string
  default     = "chaos/status.json"

  validation {
    condition     = trim(var.status_key, "/") == var.status_key && var.status_key != ""
    error_message = "status_key must be a non-empty key without leading or trailing slashes."
  }
}

variable "image_repository" {
  description = "Chaos-agent image repository."
  type        = string
  default     = "ghcr.io/tonbo-io/ursula-chaos-agent"
}

variable "image_tag" {
  description = "Explicit immutable chaos-agent image tag."
  type        = string

  validation {
    condition     = trimspace(var.image_tag) != "" && var.image_tag != "latest"
    error_message = "image_tag must be explicit and must not be latest."
  }
}

variable "server_fullname" {
  description = "Full name of the Ursula StatefulSet and client Service."
  type        = string
  default     = "ursula"
}

variable "headless_service" {
  description = "Headless Service used for stable voter Pod DNS."
  type        = string
  default     = "ursula-headless"
}

variable "indexer_service" {
  description = "Ursula indexer Service name."
  type        = string
  default     = "ursula-indexer"
}

variable "tags" {
  description = "Additional tags for the chaos-agent IAM role."
  type        = map(string)
  default     = {}
}
