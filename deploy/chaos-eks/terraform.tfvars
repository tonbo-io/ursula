aws_region    = "us-east-1"
cluster_name  = "ursula-chaos-jul21-eks"
namespace     = "ursula"
status_bucket = "ursula-chaos-status-tonbo"
status_key    = "status.json"
image_tag     = "sha-879f089"

server_fullname  = "ursula"
headless_service = "ursula-headless"
indexer_service  = "ursula-indexer"

tags = {
  Environment = "chaos"
  Owner       = "platform"
}
