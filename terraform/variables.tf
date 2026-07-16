variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name prefix for resources"
  type        = string
  default     = "dispatch"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "demo"
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version (pinned, not latest)"
  type        = string
  default     = "1.29"
}

variable "vpc_cidr" {
  type    = string
  default = "10.0.0.0/16"
}
