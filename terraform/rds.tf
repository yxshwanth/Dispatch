resource "aws_db_subnet_group" "aurora" {
  name       = "${local.name}-aurora"
  subnet_ids = module.vpc.private_subnets
  tags       = local.tags
}

resource "aws_rds_cluster_parameter_group" "aurora" {
  name   = "${local.name}-aurora-pg16"
  family = "aurora-postgresql16"

  parameter {
    name         = "shared_preload_libraries"
    value        = "pg_stat_statements"
    apply_method = "pending-reboot"
  }

  tags = local.tags
}

resource "aws_rds_cluster" "aurora" {
  cluster_identifier = "${local.name}-aurora"
  engine             = "aurora-postgresql"
  engine_version     = "16.4"
  database_name      = "dispatch"
  master_username    = "dispatch"

  # Prefer RDS-managed password in Secrets Manager (not plaintext in state).
  manage_master_user_password = true

  db_subnet_group_name            = aws_db_subnet_group.aurora.name
  vpc_security_group_ids          = [aws_security_group.rds.id]
  db_cluster_parameter_group_name = aws_rds_cluster_parameter_group.aurora.name

  storage_encrypted   = true
  skip_final_snapshot = true
  deletion_protection = false

  backup_retention_period = 7
  preferred_backup_window = "07:00-09:00"

  tags = local.tags
}

resource "aws_rds_cluster_instance" "aurora" {
  count              = 2
  identifier         = "${local.name}-aurora-${count.index}"
  cluster_identifier = aws_rds_cluster.aurora.id
  instance_class     = "db.t4g.medium"
  engine             = aws_rds_cluster.aurora.engine
  engine_version     = aws_rds_cluster.aurora.engine_version

  tags = local.tags
}
