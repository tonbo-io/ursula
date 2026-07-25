terraform {
  backend "s3" {
    bucket       = "ursula-opentofu-state-232814779190-us-east-1"
    key          = "clusters/ursula-chaos-jul21/eks.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}
