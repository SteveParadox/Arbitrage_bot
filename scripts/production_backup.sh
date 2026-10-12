#!/bin/sh
set -eu
: "${ARB_SECRETS_DIR:?mounted secret directory required}"
: "${ARB_CORS_ORIGINS:?HTTPS origins required}"
: "${ARB_TRUSTED_HOSTS:?trusted hosts required}"
compose=docker/production/compose.yml
docker compose -f "$compose" stop engine-control api integrity
docker compose -f "$compose" --profile operations run --rm --no-deps backup backup \
  --key-file /run/secrets/backup-key --state /app/data --output /backups \
  --status-file /app/data/observability/backup.json --source-commit "$(git rev-parse HEAD)"
docker compose -f "$compose" up -d engine-control api integrity
# Restart requires explicit execution reauthorization; no start command is sent here.
