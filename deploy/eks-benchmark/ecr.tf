# One repository per image. Existing repositories can be adopted with:
#   terraform import aws_ecr_repository.ml_api   predictive-autoscaler/ml-api
#   terraform import aws_ecr_repository.operator predictive-autoscaler/operator
resource "aws_ecr_repository" "ml_api" {
  name                 = "predictive-autoscaler/ml-api"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  image_scanning_configuration { scan_on_push = false }
}

resource "aws_ecr_repository" "operator" {
  name                 = "predictive-autoscaler/operator"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  image_scanning_configuration { scan_on_push = false }
}
