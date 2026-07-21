output "chaos_agent_role_arn" {
  value = aws_iam_role.chaos_agent.arn
}

output "generated_values_file" {
  value = local_file.helm_values.filename
}

output "helm_install" {
  value = "helm upgrade --install ursula-chaos ../../charts/ursula-chaos --namespace ${var.namespace} -f generated-values.yaml"
}
