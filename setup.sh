#!/usr/bin/env bash
# Bring up OpenBao and a Kafka broker that accepts OAUTHBEARER tokens signed by
# OpenBao, then start a long-lived producer whose token OpenBao Agent refreshes.
# See README.md.
set -euo pipefail
cd "$(dirname "$0")"

log() { printf '==> %s\n' "$*"; }
bao() { docker compose exec -T openbao bao "$@"; }

issue() { # issue <role> <cn> <ttl> <out.pem>
  bao write -format=json "pki/issue/$1" common_name="$2" ttl="$3" \
    private_key_format=pkcs8 \
    | jq -r '.data.private_key, .data.certificate' >"$4"
}

log "Starting OpenBao (dev mode)"
docker compose up -d openbao
until bao status >/dev/null 2>&1; do sleep 1; done

log "Configuring PKI, OIDC signing key, role, policy, AppRole and entity"
docker compose exec -T openbao sh -s <scripts/configure-openbao.sh

log "Issuing broker and admin certificates"
mkdir -p certs/broker certs/admin tokens/billing-service agent/billing-service
bao read -field=certificate pki/cert/ca >certs/ca.pem
for d in certs/broker certs/admin tokens/billing-service; do cp certs/ca.pem "$d/ca.pem"; done
issue kafka-broker kafka 720h certs/broker/kafka.pem
issue kafka-admin kafka-admin 8h certs/admin/admin.pem

log "Writing AppRole credentials for billing-service"
bao read -field=role_id auth/approle/role/billing-service/role-id >agent/billing-service/role_id
bao write -f -field=secret_id auth/approle/role/billing-service/secret-id >agent/billing-service/secret_id

log "Starting Kafka"
docker compose --profile kafka up -d kafka
kadmin() { docker compose exec -T kafka "/opt/kafka/bin/$1" --bootstrap-server kafka:9094 --command-config /etc/kafka/admin.properties "${@:2}"; }
until kadmin kafka-topics.sh --list >/dev/null 2>&1; do sleep 2; done

log "Creating topics and ACLs"
for t in billing.events payments.events; do
  kadmin kafka-topics.sh --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 2>/dev/null
done
kadmin kafka-acls.sh --add --allow-principal User:billing-service \
  --operation Write --operation Describe --topic billing.events

log "Starting billing-agent and billing-app"
docker compose --profile apps up -d billing-agent billing-app

log "Done. Run ./test.sh"
