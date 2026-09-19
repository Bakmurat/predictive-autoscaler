data "aws_availability_zones" "available" {
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 2)
}

# Public subnets only, no NAT gateway: nodes get public IPs and egress through the internet gateway.
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.21"

  name = "${var.cluster_name}-vpc"
  cidr = var.vpc_cidr
  azs  = local.azs

  public_subnets          = [cidrsubnet(var.vpc_cidr, 3, 0), cidrsubnet(var.vpc_cidr, 3, 1)]
  map_public_ip_on_launch = true
  enable_nat_gateway      = false
  enable_dns_hostnames    = true
  enable_dns_support      = true

  public_subnet_tags = {
    "kubernetes.io/role/elb"                    = "1"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
}
