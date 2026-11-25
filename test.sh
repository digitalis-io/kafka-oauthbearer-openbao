#!/usr/bin/env bash
# Exercise the lab: authorisation, where the principal comes from, forged
# tokens, refresh on a long-lived connection and what happens when the token
# stops being renewed. Takes about six minutes.
set -euo pipefail
cd "$(dirname "$0")"

pass=0 fail=0
ok()  { printf '  PASS  %s\n' "$*"; pass=$((pass + 1)); }
bad() { printf '  FAIL  %s\n' "$*"; fail=$((fail + 1)); }
log() { printf '\n==> %s\n' "$*"; }

JWT=tokens/billing-service/billing-service.jwt
claims() { cut -d. -f2 "${1:-$JWT}" | tr '_-' '/+' | awk '{ while (length($0) % 4) $0 = $0 "="; print }' | base64 -d; }
b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }
app_errors() { docker compose logs --since "$1" billing-app 2>&1 | grep -cE 'ERROR|Exception' || true; }
app_sends()  { docker compose logs --since "$1" billing-app 2>&1 | grep -c producer_send_success || true; }

# produce <topic> [token file inside /tokens or /forged]
produce() {
  local token=${2:-/tokens/billing-service.jwt}
  docker compose run --rm --no-deps -T --entrypoint bash \
    -e KAFKA_OPTS="-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file://${token}" \
    -v "$PWD/tokens/forged:/forged:ro" billing-app -c "
      sed 's#file:///tokens/billing-service.jwt#file://${token}#' /etc/client/billing.properties >/tmp/client.properties
      echo test-\$(date +%s) | /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server kafka:9092 --topic $1 \
        --producer.config /tmp/client.properties \
        --producer-property max.block.ms=15000 2>&1" || true
}

log "billing-service may write to billing.events"
out=$(produce billing.events)
if ! grep -qE 'ERROR|Exception' <<<"$out"; then ok "write accepted"; else bad "write rejected: $out"; fi

log "billing-service may not write to payments.events"
out=$(produce payments.events)
if grep -q 'TopicAuthorizationException' <<<"$out"; then ok "denied: $(grep -m1 -o 'Not authorized[^.]*' <<<"$out")"; else bad "expected TopicAuthorizationException: $out"; fi

log "The principal comes from the OpenBao entity, not from the application"
principal=$(claims | jq -r .kafka_principal)
[ "$principal" = billing-service ] && ok "token claim kafka_principal=$principal" || bad "kafka_principal=$principal"
token=$(docker compose exec -T openbao bao write -field=token auth/approle/login \
  role_id="$(cat agent/billing-service/role_id)" secret_id="$(cat agent/billing-service/secret_id)")
out=$(docker compose exec -T -e BAO_TOKEN="$token" openbao \
  bao write identity/entity/name/billing-service name=payments-service 2>&1 || true)
if grep -q 'permission denied' <<<"$out"; then ok "application cannot rename its own entity"; else bad "$out"; fi

log "A token signed by any other key is rejected"
mkdir -p tokens/forged
openssl genrsa -out tokens/forged/key.pem 2048 2>/dev/null
kid=$(cut -d. -f1 "$JWT" | tr '_-' '/+' | awk '{ while (length($0) % 4) $0 = $0 "="; print }' | base64 -d | jq -r .kid)
now=$(date +%s)
h=$(jq -cn --arg kid "$kid" '{alg:"RS256",typ:"JWT",kid:$kid}' | b64url)
p=$(claims | jq -c --argjson now "$now" '.iat=$now | .exp=($now+300)' | b64url)
s=$(printf '%s.%s' "$h" "$p" | openssl dgst -sha256 -sign tokens/forged/key.pem | b64url)
printf '%s.%s.%s' "$h" "$p" "$s" >tokens/forged/forged.jwt
out=$(produce billing.events /forged/forged.jwt)
if grep -q 'invalid_token' <<<"$out"; then ok "forged token refused: invalid_token"; else bad "forged token accepted: $out"; fi

log "A long-lived producer keeps working across token refreshes"
start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
first_exp=$(claims | jq .exp)
echo "  current token expires $(date -r "$first_exp" +%T 2>/dev/null || date -d "@$first_exp" +%T), watching for 2.5 minutes"
sleep 150
sends=$(app_sends "$start"); errors=$(app_errors "$start")
if [ "$errors" -eq 0 ] && [ "$sends" -gt 100 ] && [ "$(claims | jq .exp)" -gt "$first_exp" ]; then
  ok "$sends messages, 0 errors, token replaced on disk"
else bad "$sends messages, $errors errors"; fi

log "The same connection is cut off once the token stops being renewed"
docker compose stop billing-agent >/dev/null 2>&1
exp=$(claims | jq .exp)
echo "  agent stopped, token expires $(date -r "$exp" +%T 2>/dev/null || date -d "@$exp" +%T)"
start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
deadline=$(( exp + 120 ))
while [ "$(app_errors "$start")" -eq 0 ] && [ "$(date +%s)" -lt "$deadline" ]; do sleep 5; done
if [ "$(app_errors "$start")" -gt 0 ]; then
  ok "producer failed $(( $(date +%s) - exp ))s after expiry: $(docker compose logs --since "$start" billing-app 2>&1 | grep -m1 -oE '[A-Za-z]+Exception[^,]{0,80}')"
else bad "producer still sending 120s after token expiry"; fi
docker compose --profile apps start billing-agent >/dev/null 2>&1
docker compose --profile apps restart billing-app >/dev/null 2>&1

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
