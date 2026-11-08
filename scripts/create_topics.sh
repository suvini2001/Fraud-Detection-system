#!/usr/bin/env bash
# Creates all topics with explicit partitions, replication and retention.
# Idempotent (--if-not-exists), so it is safe to run on every start.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka-1:29092,kafka-2:29092,kafka-3:29092}"

create() {
  local name=$1 partitions=$2 retention_ms=$3
  for attempt in 1 2 3 4 5; do
    if kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic "$name" \
        --partitions "$partitions" \
        --replication-factor 3 \
        --config min.insync.replicas=2 \
        --config retention.ms="$retention_ms" \
        --config compression.type=producer; then
      echo "  ✓ $name  (partitions=$partitions, RF=3, min.insync=2)"
      return 0
    fi
    echo "  ... $name failed (attempt $attempt/5), retrying in 5s"
    sleep 5
  done
  echo "  ✗ $name could not be created" >&2
  return 1
}

echo "Creating topics on $BOOTSTRAP ..."
#        topic                partitions  retention
create   transactions.raw     6           604800000     # 7 days  - input stream, scaled by account key
create   transactions.scored  6           604800000     # 7 days  - every scored transaction
create   fraud.alerts         3           2592000000    # 30 days - fraud only (low volume, kept longer)
create   transactions.dlq     3           1209600000    # 14 days - poison messages for inspection/replay

echo
kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic transactions.raw || true
echo "Topics ready."
