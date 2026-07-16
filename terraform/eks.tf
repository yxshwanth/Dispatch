module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = "${local.name}-eks"
  cluster_version = var.kubernetes_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_endpoint_public_access = true
  enable_irsa                    = true

  eks_managed_node_groups = {
    default = {
      instance_types = ["t3.medium"]
      min_size       = 1
      max_size       = 3
      desired_size   = 2
    }
  }

  tags = local.tags
}

# IRSA: API pod — S3 payload archival write
data "aws_iam_policy_document" "api_s3" {
  statement {
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:AbortMultipartUpload",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.payloads.arn,
      "${aws_s3_bucket.payloads.arn}/*",
    ]
  }
}

resource "aws_iam_policy" "api_s3" {
  name   = "${local.name}-api-s3"
  policy = data.aws_iam_policy_document.api_s3.json
  tags   = local.tags
}

module "api_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name = "${local.name}-api"

  role_policy_arns = {
    s3 = aws_iam_policy.api_s3.arn
  }

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["dispatch:dispatch-api"]
    }
  }

  tags = local.tags
}

# IRSA: consumer pod — MSK IAM connect (and optional S3 read)
data "aws_iam_policy_document" "consumer_msk" {
  statement {
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeGroup",
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
      "kafka-cluster:WriteData",
      "kafka-cluster:DescribeClusterDynamicConfiguration",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "consumer_msk" {
  name   = "${local.name}-consumer-msk"
  policy = data.aws_iam_policy_document.consumer_msk.json
  tags   = local.tags
}

module "consumer_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name = "${local.name}-consumer"

  role_policy_arns = {
    msk = aws_iam_policy.consumer_msk.arn
  }

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["dispatch:dispatch-consumer"]
    }
  }

  tags = local.tags
}
