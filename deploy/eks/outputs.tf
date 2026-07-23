resource "local_file" "helm_values" {
  filename        = "${path.module}/generated-values.yaml"
  file_permission = "0644"
  content = yamlencode({
    fullnameOverride = var.release_name
    global = {
      image = {
        repository = var.image_repository
        tag        = var.image_tag
        pullPolicy = "IfNotPresent"
      }
    }
    serviceAccount = {
      create = true
      name   = local.server_sa
      annotations = {
        "eks.amazonaws.com/role-arn" = aws_iam_role.server.arn
      }
    }
    server = {
      replicaCount = 3
      coreCount    = var.server_core_count
      resources = {
        requests = {
          cpu                 = var.server_cpu_request
          memory              = var.server_memory_request
          "ephemeral-storage" = "2Gi"
        }
        limits = {
          memory              = var.server_memory_limit
          "ephemeral-storage" = "4Gi"
        }
      }
      scheduling = {
        topologySpreadConstraints = [{
          maxSkew           = 1
          topologyKey       = "topology.kubernetes.io/zone"
          whenUnsatisfiable = "DoNotSchedule"
          labelSelector = {
            matchLabels = {
              "app.kubernetes.io/name"     = "ursula"
              "app.kubernetes.io/instance" = var.release_name
            }
          }
        }]
      }
    }
    raft = {
      groupCount             = var.raft_group_count
      initMembershipPerGroup = var.raft_init_membership_per_group
      storageMode            = "logDir"
    }
    persistence = {
      enabled          = true
      storageClassName = kubernetes_storage_class_v1.gp3.metadata[0].name
      size             = var.raft_volume_size
    }
    s3 = {
      bucket = aws_s3_bucket.ursula.id
      region = var.aws_region
      prefix = local.server_prefix
    }
    coldStorage = {
      enabled = true
      compaction = {
        enabled           = var.cold_compaction_enabled
        maxStreamsPerPass = var.cold_compaction_max_streams_per_pass
      }
    }
    snapshotStore = {
      backend = "s3"
    }
    gateway = {
      replicaCount = var.gateway_replicas
      podDisruptionBudget = {
        enabled = true
      }
    }
    indexer = {
      enabled      = true
      replicaCount = var.indexer_replicas
      s3 = {
        prefix = local.index_prefix
      }
      serviceAccount = {
        create = true
        name   = local.indexer_sa
        annotations = {
          "eks.amazonaws.com/role-arn" = aws_iam_role.indexer.arn
        }
      }
      podDisruptionBudget = {
        enabled = true
      }
    }
  })
}

resource "local_file" "kubeconfig" {
  filename        = "${path.module}/kubeconfig"
  file_permission = "0600"
  content = yamlencode({
    apiVersion = "v1"
    kind       = "Config"
    clusters = [{
      name = module.eks.cluster_name
      cluster = {
        server                       = module.eks.cluster_endpoint
        "certificate-authority-data" = module.eks.cluster_certificate_authority_data
      }
    }]
    users = [{
      name = module.eks.cluster_name
      user = {
        exec = {
          apiVersion = "client.authentication.k8s.io/v1beta1"
          command    = "aws"
          args       = ["eks", "get-token", "--cluster-name", module.eks.cluster_name, "--region", var.aws_region]
        }
      }
    }]
    contexts = [{
      name = module.eks.cluster_name
      context = {
        cluster = module.eks.cluster_name
        user    = module.eks.cluster_name
      }
    }]
    "current-context" = module.eks.cluster_name
  })
}

output "cluster_name" {
  value = module.eks.cluster_name
}

output "aws_region" {
  value = var.aws_region
}

output "s3_bucket" {
  value = aws_s3_bucket.ursula.id
}

output "generated_values_file" {
  value = local_file.helm_values.filename
}

output "kubeconfig_file" {
  value = local_file.kubeconfig.filename
}

output "helm_install" {
  value = "KUBECONFIG=${local_file.kubeconfig.filename} helm install ${var.release_name} ../../charts/ursula --namespace ${var.namespace} --create-namespace -f generated-values.yaml"
}

output "helm_test" {
  value = "KUBECONFIG=${local_file.kubeconfig.filename} helm test ${var.release_name} --namespace ${var.namespace}"
}
