#!/bin/sh
# Runs inside the openbao container. PKI for the broker and admin, an OIDC key
# and role that sign Kafka tokens, and an AppRole bound to a named entity.
set -eu

bao secrets enable pki
bao secrets tune -max-lease-ttl=87600h pki
bao write -field=certificate pki/root/generate/internal \
  common_name="Kafka Lab Root CA" key_type=ec key_bits=256 ttl=87600h >/dev/null
bao write pki/roles/kafka-broker \
  allowed_domains=kafka,localhost allow_bare_domains=true allow_subdomains=false \
  allow_localhost=true server_flag=true client_flag=true \
  key_type=ec key_bits=256 max_ttl=720h
bao write pki/roles/kafka-admin \
  allowed_domains=kafka-admin allow_bare_domains=true allow_subdomains=false \
  server_flag=false client_flag=true key_type=ec key_bits=256 max_ttl=8h

bao write identity/oidc/config issuer=http://openbao:8200
bao write identity/oidc/key/kafka \
  algorithm=RS256 allowed_client_ids=kafka rotation_period=24h verification_ttl=24h

# One role for every application. The principal comes from the caller's entity
# name, so an application cannot ask for somebody else's identity.
bao write identity/oidc/role/kafka \
  key=kafka client_id=kafka ttl=2m \
  template='{"kafka_principal": {{identity.entity.name}}}'

bao policy write kafka-client - <<'POL'
path "identity/oidc/token/kafka" {
  capabilities = ["read"]
}
POL

bao auth enable approle
bao write auth/approle/role/billing-service \
  token_policies=kafka-client token_ttl=15m token_max_ttl=1h

# AppRole logins are aliased by role_id. Pin that alias to an entity called
# billing-service so the token claim carries a readable name.
role_id=$(bao read -field=role_id auth/approle/role/billing-service/role-id)
accessor=$(bao auth list -format=json | sed -n 's/.*"accessor": "\(auth_approle_[^"]*\)".*/\1/p')
entity_id=$(bao write -field=id identity/entity name=billing-service)
bao write identity/entity-alias name="$role_id" mount_accessor="$accessor" canonical_id="$entity_id" >/dev/null
