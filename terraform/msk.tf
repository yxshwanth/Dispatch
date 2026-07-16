# Demo uses kafka.t3.small. Production-like workloads should use kafka.m5.large (or larger).
resource "aws_msk_cluster" "this" {
  cluster_name           = "${local.name}-msk"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = module.vpc.private_subnets
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = 20
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  tags = local.tags
}

resource "aws_msk_configuration" "this" {
  name              = "${local.name}-msk-config"
  kafka_versions    = ["3.6.0"]
  server_properties = <<-PROPERTIES
    auto.create.topics.enable = false
    delete.topic.enable = true
  PROPERTIES
}

# Topics (ingest + retry) are created after apply via scripts/create-msk-topics.sh
# because auto-create is disabled and IAM-auth topic creation needs live brokers.
