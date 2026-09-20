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
  description = "EKS Kubernetes minor. 1.34 is the current standard-support line; 1.31-1.33 are in extended support (control plane billed at a higher rate)."
  type        = string
  default     = "1.34"
}

# Default addon versions for Kubernetes 1.34 (aws eks describe-addon-versions --kubernetes-version 1.34, 2026-09-20).
variable "addon_versions" {
  type = map(string)
  default = {
    "vpc-cni"            = "v1.22.4-eksbuild.3"
    "coredns"            = "v1.12.4-eksbuild.38"
    "kube-proxy"         = "v1.34.6-eksbuild.29"
    "metrics-server"     = "v0.9.0-eksbuild.11"
    "aws-ebs-csi-driver" = "v1.66.0-eksbuild.1"
  }
}

variable "vpc_cidr" {
  type    = string
  default = "10.50.0.0/16"
}

variable "instance_type" {
  description = "Node instance type (arm64 by default; images are built for the node architecture)"
  type        = string
  default     = "t4g.medium"
}

variable "node_min" {
  type    = number
  default = 2
}

variable "node_desired" {
  type    = number
  default = 2
}

variable "node_max" {
  type    = number
  default = 2
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
