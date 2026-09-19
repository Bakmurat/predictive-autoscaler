output "cluster_name" { value = module.eks.cluster_name }
output "region" { value = var.region }

output "kubeconfig_command" {
  value = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.region} --alias ${module.eks.cluster_name}"
}

output "ecr_registry" {
  description = "Registry host, e.g. <account>.dkr.ecr.<region>.amazonaws.com"
  value       = split("/", aws_ecr_repository.ml_api.repository_url)[0]
}
output "ecr_ml_api_url" { value = aws_ecr_repository.ml_api.repository_url }
output "ecr_operator_url" { value = aws_ecr_repository.operator.repository_url }

output "prometheus_url_in_cluster" {
  value = "http://kps-kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090"
}
