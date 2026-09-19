resource "kubernetes_namespace" "istio_system" {
  metadata { name = "istio-system" }
  depends_on = [module.eks]
}

resource "kubernetes_namespace" "demo" {
  metadata {
    name   = "demo"
    labels = { istio-injection = "enabled" }
  }
  depends_on = [module.eks]
}

resource "kubernetes_namespace" "monitoring" {
  metadata { name = "monitoring" }
  depends_on = [module.eks]
}

resource "helm_release" "istio_base" {
  name       = "istio-base"
  repository = "https://istio-release.storage.googleapis.com/charts"
  chart      = "base"
  version    = var.chart_versions.istio
  namespace  = kubernetes_namespace.istio_system.metadata[0].name
  wait       = true
}

resource "helm_release" "istiod" {
  name       = "istiod"
  repository = "https://istio-release.storage.googleapis.com/charts"
  chart      = "istiod"
  version    = var.chart_versions.istio
  namespace  = kubernetes_namespace.istio_system.metadata[0].name
  values     = [file("${path.module}/helm/istiod-values.yaml")]
  wait       = true
  timeout    = 600
  depends_on = [helm_release.istio_base]
}

resource "helm_release" "keda" {
  name             = "keda"
  repository       = "https://kedacore.github.io/charts"
  chart            = "keda"
  version          = var.chart_versions.keda
  namespace        = "keda"
  create_namespace = true
  values           = [file("${path.module}/helm/keda-values.yaml")]
  wait             = true
  timeout          = 600
  depends_on       = [module.eks]
}

resource "helm_release" "kube_prometheus_stack" {
  name       = "kps"
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-prometheus-stack"
  version    = var.chart_versions.kube_prometheus_stack
  namespace  = kubernetes_namespace.monitoring.metadata[0].name
  values     = [file("${path.module}/helm/kps-values.yaml")]
  wait       = true
  timeout    = 900

  set_sensitive {
    name  = "grafana.adminPassword"
    value = var.grafana_admin_password
  }
  set {
    name  = "prometheus.prometheusSpec.retention"
    value = var.prometheus_retention
  }
  set {
    name  = "prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.resources.requests.storage"
    value = var.prometheus_storage
  }
  depends_on = [kubernetes_storage_class.gp3]
}
