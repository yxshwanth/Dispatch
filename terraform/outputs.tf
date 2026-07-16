output "vpc_id" {
  value = module.vpc.vpc_id
}

output "eks_cluster_name" {
  value = module.eks.cluster_name
}

output "eks_cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "aurora_cluster_endpoint" {
  description = "Use this writer endpoint in DATABASE_URL (not an instance endpoint)"
  value       = aws_rds_cluster.aurora.endpoint
}

output "aurora_reader_endpoint" {
  description = "Optional read replica endpoint"
  value       = aws_rds_cluster.aurora.reader_endpoint
}

output "msk_bootstrap_brokers_sasl_iam" {
  value = aws_msk_cluster.this.bootstrap_brokers_sasl_iam
}

output "redis_primary_endpoint" {
  value = aws_elasticache_cluster.redis.cache_nodes[0].address
}

output "s3_payloads_bucket" {
  value = aws_s3_bucket.payloads.bucket
}

output "api_irsa_role_arn" {
  value = module.api_irsa.iam_role_arn
}

output "consumer_irsa_role_arn" {
  value = module.consumer_irsa.iam_role_arn
}
