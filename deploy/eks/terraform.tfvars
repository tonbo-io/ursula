name       = "ursula-chaos-jul21"
aws_region = "us-east-1"
image_tag  = "sha-2de400a"

cluster_endpoint_public_access_cidrs = ["8.216.133.37/32"]

single_nat_gateway  = true
node_instance_types = ["m6i.xlarge"]
nodes_per_az        = 1
max_nodes_per_az    = 2

server_core_count                     = 2
server_cpu_request                    = "1500m"
server_memory_request                 = "6Gi"
server_memory_limit                   = "10Gi"
raft_group_count                      = 256
raft_init_membership_per_group        = false
raft_volume_size                      = "50Gi"
gateway_replicas                      = 2
indexer_replicas                      = 2
s3_noncurrent_version_expiration_days = 1
cold_compaction_enabled               = true
cold_compaction_max_streams_per_pass  = 64

tags = {
  Environment = "chaos"
  Owner       = "platform"
}
