#!/usr/bin/env bash
# ==============================================================================
# Lab Verification Suite (OAuthBearer / JWT)
# Exercises Kafka topic authorization, OpenBao entity principal binding,
# JWT signature verification, live token refresh, and expiry disconnection.
# Estimated runtime: ~6 minutes
# ==============================================================================

set -euo pipefail
cd "$(dirname "$0")"

# --- Configuration & Constants ------------------------------------------------
readonly JWT_PATH="tokens/billing-service/billing-service.jwt"
readonly FORGED_DIR="tokens/forged"
readonly REFRESH_WATCH_SEC=150
readonly EXPIRY_GRACE_SEC=120

# --- Terminal Formatting ------------------------------------------------------
if [[ -t 1 ]]; then
  readonly GREEN='\033[0;32m'
  readonly RED='\033[0;31m'
  readonly BLUE='\033[0;34m'
  readonly DIM='\033[2m'
  readonly NC='\033[0m'
else
  readonly GREEN='' RED='' BLUE='' DIM='' NC=''
fi

PASS_COUNT=0
FAIL_COUNT=0

# --- Cleanup Trap -------------------------------------------------------------
cleanup() {
  rm -rf "$FORGED_DIR"
  # Ensure services stopped/affected during expiry tests are restored cleanly
  docker compose --profile apps start billing-agent >/dev/null 2>&1 || true
  docker compose --profile apps restart billing-app >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- Logging & Assertion Helpers ----------------------------------------------
section() {
  printf "\n${BLUE}==> %s${NC}\n" "$1"
}

info() {
  printf "  ${DIM}%s${NC}\n" "$1"
}

pass() {
  printf "  ${GREEN}[PASS]${NC} %s\n" "$1"
  PASS_COUNT=$((PASS_COUNT + 1))
}

fail() {
  printf "  ${RED}[FAIL]${NC} %s\n" "$1"
  FAIL_COUNT=$((FAIL_COUNT + 1))
}

assert_contains() {
  local haystack="$1" needle="$2" success_msg="$3" fail_msg="$4"
  if grep -qE "$needle" <<<"$haystack"; then
    pass "$success_msg"
  else
    fail "$fail_msg (Output:$haystack)"
  fi
}

assert_not_contains() {
  local haystack="$1" needle="$2" success_msg="$3" fail_msg="$4"
  if ! grep -qE "$needle" <<<"$haystack"; then
    pass "$success_msg"
  else
    fail "$fail_msg (Output:$haystack)"
  fi
}

# --- Domain Helpers -----------------------------------------------------------

# Decodes part 1 (header) or part 2 (claims) of a JWT file
decode_jwt_part() {
  local part_index="$1"
  local file="${2:-$JWT_PATH}"
  cut -d. -f"$part_index" "$file" \
    | tr '_-' '/+' \
    | awk '{ while (length($0) % 4) $0 = $0 "="; print }' \
    | base64 -d
}

encode_b64url() {
  openssl base64 -A | tr '+/' '-_' | tr -d '='
}

format_epoch() {
  local epoch="$1"
  date -r "$epoch" +%T 2>/dev/null || date -d "@$epoch" +%T
}

count_app_errors_since() {
  docker compose logs --since "$1" billing-app 2>&1 | grep -cE 'ERROR|Exception' || true
}

count_app_sends_since() {
  docker compose logs --since "$1" billing-app 2>&1 | grep -c 'producer_send_success' || true
}

# Sends a single timestamped message to a Kafka topic using a specified JWT
produce_message() {
  local topic="$1"
  local container_token_path="${2:-/tokens/billing-service.jwt}"
  local msg
  msg="test-$(date +%s)"

  docker compose run --rm --no-deps -T --entrypoint bash \
    -e KAFKA_OPTS="-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file://${container_token_path}" \
    -v "$PWD/$FORGED_DIR:/forged:ro" billing-app -s "$topic" "$container_token_path" "$msg" <<'EOF' || true
      topic="$1"
      token_path="$2"
      msg="$3"

      sed "s#file:///tokens/billing-service.jwt#file://${token_path}#" \
        /etc/client/billing.properties > /tmp/client.properties

      printf '%s\n' "$msg" | /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server kafka:9092 \
        --topic "$topic" \
        --producer.config /tmp/client.properties \
        --producer-property max.block.ms=15000 2>&1
EOF
}

# --- Test Scenarios -----------------------------------------------------------

test_kafka_authorization() {
  section "1. Kafka Topic Authorization"

  local out
  out=$(produce_message "billing.events")
  assert_not_contains "$out" "ERROR|Exception" \
    "Allowed to write to 'billing.events'" \
    "Unexpectedly rejected from 'billing.events'"

  out=$(produce_message "payments.events")
  assert_contains "$out" "TopicAuthorizationException" \
    "Denied write access to 'payments.events' (TopicAuthorizationException)" \
    "Expected TopicAuthorizationException for 'payments.events'"
}

test_openbao_principal_binding() {
  section "2. OpenBao Entity Principal Binding"

  local principal role_id secret_id token out
  principal=$(decode_jwt_part 2 | jq -r '.kafka_principal')

  if [[ "$principal" == "billing-service" ]]; then
    pass "JWT claim 'kafka_principal' is bound to '$principal'"
  else
    fail "Expected kafka_principal='billing-service', got '$principal'"
  fi

  role_id=$(<agent/billing-service/role_id)
  secret_id=$(<agent/billing-service/secret_id)
  token=$(docker compose exec -T openbao bao write -field=token \
    auth/approle/login role_id="$role_id" secret_id="$secret_id")

  # Verify the application cannot rename its own OpenBao identity entity
  out=$(docker compose exec -T -e BAO_TOKEN="$token" openbao \
    bao write identity/entity/name/billing-service name=payments-service 2>&1 || true)

  assert_contains "$out" "permission denied" \
    "Application cannot rename its own OpenBao identity entity" \
    "Application was unexpectedly permitted to rename its entity"
}

test_forged_token_rejection() {
  section "3. JWT Signature Verification (Forged Token)"

  mkdir -p "$FORGED_DIR"
  openssl genrsa -out "$FORGED_DIR/key.pem" 2048 2>/dev/null

  local kid now header payload signature
  kid=$(decode_jwt_part 1 | jq -r '.kid')
  now=$(date +%s)

  # Reconstruct an identical header and valid-looking claims, signed with a rogue RSA key
  header=$(jq -cn --arg kid "$kid" '{alg:"RS256", typ:"JWT", kid:$kid}' | encode_b64url)
  payload=$(decode_jwt_part 2 | jq -c --argjson now "$now" '.iat=$now | .exp=($now+300)' | encode_b64url)
  signature=$(printf '%s.%s' "$header" "$payload" \
    | openssl dgst -sha256 -sign "$FORGED_DIR/key.pem" \
    | encode_b64url)

  printf '%s.%s.%s' "$header" "$payload" "$signature" > "$FORGED_DIR/forged.jwt"

  local out
  out=$(produce_message "billing.events" "/forged/forged.jwt")
  assert_contains "$out" "invalid_token" \
    "Kafka broker refused forged JWT signature (invalid_token)" \
    "Forged JWT was unexpectedly accepted"
}

test_live_token_refresh() {
  section "4. Long-Lived Producer Across Token Refreshes"

  local start_ts first_exp new_exp sends errors
  start_ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  first_exp=$(decode_jwt_part 2 | jq -r '.exp')

  info "Current token expires at $(format_epoch "$first_exp"). Monitoring for ${REFRESH_WATCH_SEC}s..."
  sleep "$REFRESH_WATCH_SEC"

  sends=$(count_app_sends_since "$start_ts")
  errors=$(count_app_errors_since "$start_ts")
  new_exp=$(decode_jwt_part 2 | jq -r '.exp')

  if (( errors == 0 && sends > 100 && new_exp > first_exp )); then
    pass "Sustained $sends messages with 0 errors; token rotated (new expiry: $(format_epoch "$new_exp"))"
  else
    fail "Refresh check failed (sends=$sends, errors=$errors, old_exp=$first_exp, new_exp=$new_exp)"
  fi
}

test_token_expiry_cutoff() {
  section "5. Connection Cutoff After Token Renewal Stops"

  docker compose stop billing-agent >/dev/null 2>&1

  local exp_epoch start_ts deadline now_epoch error_detail
  exp_epoch=$(decode_jwt_part 2 | jq -r '.exp')
  start_ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  deadline=$(( exp_epoch + EXPIRY_GRACE_SEC ))

  info "Agent stopped. Token expires at $(format_epoch "$exp_epoch"). Waiting for connection failure..."

  while (( $(count_app_errors_since "$start_ts") == 0 )); do
    now_epoch=$(date +%s)
    if (( now_epoch >= deadline )); then
      fail "Producer still sending ${EXPIRY_GRACE_SEC}s after token expiry"
      return
    fi
    sleep 5
  done

  error_detail=$(docker compose logs --since "$start_ts" billing-app 2>&1 \
    | grep -m1 -oE '[A-Za-z]+Exception[^,]{0,80}' || true)
  pass "Producer failed $(( $(date +%s) - exp_epoch ))s after expiry: ${error_detail}"
}

# --- Main ---------------------------------------------------------------------
test_kafka_authorization
test_openbao_principal_binding
test_forged_token_rejection
test_live_token_refresh
test_token_expiry_cutoff

printf '\n%d passed, %d failed\n' "$PASS_COUNT" "$FAIL_COUNT"
(( FAIL_COUNT == 0 ))
