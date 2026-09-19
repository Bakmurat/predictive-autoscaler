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

  cluster_addons = {
    vpc-cni        = { most_recent = true }
    coredns        = { most_recent = true }
    kube-proxy     = { most_recent = true }
    metrics-server = { most_recent = true }
    aws-ebs-csi-driver = {
      most_recent              = true
      service_account_role_arn = module.ebs_csi_irsa.iam_role_arn
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
      disk_size      = 40
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
