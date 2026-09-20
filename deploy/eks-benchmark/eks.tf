module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.37"

  cluster_name    = var.cluster_name
  cluster_version = var.kubernetes_version

  cluster_endpoint_public_access  = true
  cluster_endpoint_private_access = false
  enable_irsa                     = true

  # The identity running terraform gets cluster-admin through an EKS access entry.
  enable_cluster_creator_admin_permissions = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.public_subnets

  # Addon versions are pinned per Kubernetes minor (see var.addon_versions); EKS upgrades one minor at a time.
  cluster_addons = {
    vpc-cni = {
      addon_version = var.addon_versions["vpc-cni"]
      # Prefix delegation: /28 prefixes per ENI instead of single IPs, so small instances are not
      # capped at ~17 pods (t4g.medium: 3 ENIs x 6 IPs). Set before node groups are created/rolled.
      configuration_values = jsonencode({
        env = {
          ENABLE_PREFIX_DELEGATION = "true"
          WARM_PREFIX_TARGET       = "1"
        }
      })
    }
    coredns    = { addon_version = var.addon_versions["coredns"] }
    kube-proxy = { addon_version = var.addon_versions["kube-proxy"] }
    metrics-server = {
      addon_version = var.addon_versions["metrics-server"]
      # One replica and small requests: two 200Mi replicas do not fit a two-node t4g.medium cluster.
      configuration_values = jsonencode({
        replicas  = 1
        resources = { requests = { cpu = "30m", memory = "64Mi" }, limits = { memory = "160Mi" } }
      })
    }
    aws-ebs-csi-driver = {
      addon_version            = var.addon_versions["aws-ebs-csi-driver"]
      service_account_role_arn = module.ebs_csi_irsa.iam_role_arn
      # One controller replica (the default two do not fit two small nodes; a restart is tolerable here).
      configuration_values = jsonencode({
        controller = {
          replicaCount = 1
          resources    = { requests = { cpu = "10m", memory = "40Mi" }, limits = { memory = "256Mi" } }
        }
      })
    }
  }

  # The control plane must reach admission webhooks running on the nodes: Istio's sidecar injector
  # listens on 15017 (the module's defaults cover 443/4443/6443/8443/9443/10250 only).
  node_security_group_additional_rules = {
    ingress_cluster_istiod_webhook = {
      description                   = "Cluster API to istiod webhook"
      protocol                      = "tcp"
      from_port                     = 15017
      to_port                       = 15017
      type                          = "ingress"
      source_cluster_security_group = true
    }
    # The metrics-server addon serves the metrics.k8s.io APIService on 10251; without this rule the
    # API server cannot reach it and `kubectl top` / resource-metric HPAs fail.
    ingress_cluster_metrics_server = {
      description                   = "Cluster API to metrics-server"
      protocol                      = "tcp"
      from_port                     = 10251
      to_port                       = 10251
      type                          = "ingress"
      source_cluster_security_group = true
    }
  }

  eks_managed_node_groups = {
    bench = {
      ami_type       = "AL2023_ARM_64_STANDARD"
      instance_types = [var.instance_type]
      capacity_type  = "ON_DEMAND"
      min_size       = var.node_min
      desired_size   = var.node_desired
      max_size       = var.node_max
      subnet_ids     = module.vpc.public_subnets
      # 20 GiB is what the AL2023 launch template actually provisions for this group (verified with
      # describe-volumes on 2026-09-20); declared explicitly so the cost ledger and the code agree.
      disk_size      = 20
      labels         = { role = "worker" }
    }
  }

  # Cheap: no KMS envelope encryption key, no CloudWatch log groups.
  create_kms_key              = false
  cluster_encryption_config   = {}
  create_cloudwatch_log_group = false
  cluster_enabled_log_types   = []
}

module "ebs_csi_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.60"

  role_name             = "${var.cluster_name}-ebs-csi"
  attach_ebs_csi_policy = true

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:ebs-csi-controller-sa"]
    }
  }
}
