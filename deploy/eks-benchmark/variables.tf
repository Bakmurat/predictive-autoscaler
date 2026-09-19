variable "region" {
  description = "AWS region for the benchmark cluster"
  type        = string
  default     = "ap-southeast-1"
}

variable "cluster_name" {
  description = "EKS cluster name"
  type        = string
  default     = "predictive-bench"
}

variable "kubernetes_version" {
  type    = string
  default = "1.32"
}

variable "vpc_cidr" {
  type    = string
  default = "10.50.0.0/16"
}

variable "instance_type" {
  description = "Node instance type (arm64 by default; images are built for the node architecture)"
  type        = string
  default     = "t4g.large"
}

variable "node_min" {
  type    = number
  default = 2
}

variable "node_desired" {
  type    = number
  default = 3
}

variable "node_max" {
  type    = number
  default = 3
}

variable "tags" {
  description = "Tags applied to every resource"
  type        = map(string)
  default = {
    Project = "predictive-benchmark"
    Owner   = "benchmark"
  }
}

variable "grafana_admin_password" {
  description = "Grafana admin password (Grafana is reachable only through kubectl port-forward). No default on purpose."
  type        = string
  sensitive   = true
}

variable "prometheus_retention" {
  type    = string
  default = "15d"
}

variable "prometheus_storage" {
  type    = string
  default = "20Gi"
}

# Pinned chart versions used for the recorded benchmark runs.
variable "chart_versions" {
  type = map(string)
  default = {
    istio                 = "1.30.4"
    keda                  = "2.20.2"
    kube_prometheus_stack = "91.4.1"
  }
}
