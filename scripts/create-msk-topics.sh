#!/usr/bin/env bash
# Create MSK topics after terraform apply (auto-create is disabled).
# Requires AWS CLI + IAM auth for Kafka (or run from a bastion with rpk).
set -euo pipefail

CLUSTER_ARN="${1:?usage: $0 <msk-cluster-arn>}"
REGION="${AWS_REGION:-us-east-1}"

echo "Create topics dispatch.ingest (3 partitions) and dispatch.retry (1 partition) on $CLUSTER_ARN"
echo "Example with kafka-topics.sh / rpk after obtaining bootstrap brokers from:"
echo "  aws kafka get-bootstrap-brokers --cluster-arn $CLUSTER_ARN --region $REGION"
echo "Topics: dispatch.ingest, dispatch.retry"
