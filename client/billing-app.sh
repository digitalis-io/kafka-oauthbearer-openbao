#!/usr/bin/env bash
# One long-lived producer for the life of the container. It reads its token
# from the file the agent writes and re-authenticates on the same connection.
set -u

until [ -s /tokens/billing-service.jwt ]; do
  echo "waiting for token"
  sleep 2
done

exec /opt/kafka/bin/kafka-verifiable-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic billing.events \
  --throughput 1 \
  --max-messages -1 \
  --producer.config /etc/client/billing.properties
