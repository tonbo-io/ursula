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
      initMembershipPerGroup = true
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
      }
      podDisruptionBudget = {
        enabled = true
      }
    }
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

output "configure_kubectl" {
  value = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.aws_region}"
}

output "helm_install" {
  value = "helm install ${var.release_name} ../../charts/ursula --namespace ${var.namespace} --create-namespace -f generated-values.yaml"
}

output "helm_test" {
  value = "helm test ${var.release_name} --namespace ${var.namespace}"
}
