#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
docker compose --profile kafka --profile apps down -v --remove-orphans
rm -rf certs tokens agent/billing-service
