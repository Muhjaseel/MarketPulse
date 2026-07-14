#!/usr/bin/env bash
# =====================================================================
# Provisions Redpanda topics by executing `rpk` natively *inside* the
# running redpanda container via `docker exec`. This removes the old
# dependency on a host-installed `rpk` or `kafka-topics.sh` binary,
# which is what caused failures on machines (e.g. plain macOS terminals)
# that never had the Kafka CLI tools installed locally.
# =====================================================================
set -euo pipefail

REDPANDA_CONTAINER="${REDPANDA_CONTAINER_NAME:-marketpulse_redpanda}"
BOOTSTRAP_SERVER="${REDPANDA_INTERNAL_BROKER:-localhost:9092}"
TOPICS=("market_prices" "social_sentiment")
PARTITIONS="${REDPANDA_TOPIC_PARTITIONS:-3}"
REPLICATION="${REDPANDA_TOPIC_REPLICATION:-1}"
MAX_WAIT_SECONDS="${BOOTSTRAP_MAX_WAIT_SECONDS:-60}"

if ! command -v docker &> /dev/null; then
  echo "❌ Docker CLI not found on this host. Install Docker Desktop / Docker Engine first." >&2
  exit 1
fi

echo "⏳ Waiting for container '${REDPANDA_CONTAINER}' to report a healthy cluster..."

elapsed=0
until docker exec "${REDPANDA_CONTAINER}" rpk cluster health 2>/dev/null | grep -q "Healthy:.*true"; do
  if (( elapsed >= MAX_WAIT_SECONDS )); then
    echo "❌ Timed out after ${MAX_WAIT_SECONDS}s waiting for Redpanda to become healthy." >&2
    echo "   Check 'docker compose logs redpanda' for details." >&2
    exit 1
  fi
  sleep 2
  elapsed=$((elapsed + 2))
done

echo "🔌 Cluster is healthy. Provisioning topics natively inside the container..."

for TOPIC in "${TOPICS[@]}"; do
  echo "📦 Ensuring topic exists: [ ${TOPIC} ] (partitions=${PARTITIONS}, replication=${REPLICATION})"
  if docker exec "${REDPANDA_CONTAINER}" rpk topic create "${TOPIC}" \
      --brokers "${BOOTSTRAP_SERVER}" \
      -p "${PARTITIONS}" \
      -r "${REPLICATION}" 2>&1 | tee /tmp/rpk_create_output.log | grep -qi "already exists"; then
    echo "   ↳ Topic '${TOPIC}' already existed — no-op."
  fi
done

echo "✅ Topic bootstrap completed successfully. No host-side kafka CLI tools were required."
