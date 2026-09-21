#!/bin/sh
set -e

apk add --no-cache curl jq >/dev/null

METABASE_URL="http://metabase:3000"

echo "Checking Metabase setup status..."
SETUP_TOKEN=$(curl -s "$METABASE_URL/api/session/properties" | jq -r '.["setup-token"]')

if [ "$SETUP_TOKEN" = "null" ] || [ -z "$SETUP_TOKEN" ]; then
  echo "Metabase is already set up, skipping."
  exit 0
fi

echo "Creating admin user..."

SETUP_PAYLOAD=$(jq -n \
  --arg token "$SETUP_TOKEN" \
  --arg email "$MB_ADMIN_EMAIL" \
  --arg password "$MB_ADMIN_PASSWORD" \
  --arg site_name "$MB_SITE_NAME" \
  '{
    token: $token,
    user: {
      first_name: "Admin",
      last_name: "User",
      email: $email,
      password: $password
    },
    prefs: {
      site_name: $site_name,
      site_locale: "en",
      allow_tracking: false
    }
  }')

HTTP_STATUS=$(curl -s -o /tmp/setup_response.json -w "%{http_code}" -X POST "$METABASE_URL/api/setup" \
  -H "Content-Type: application/json" \
  -d "$SETUP_PAYLOAD")

if [ "$HTTP_STATUS" != "200" ]; then
  echo "Metabase setup failed with HTTP status $HTTP_STATUS"
  cat /tmp/setup_response.json
  exit 1
fi
echo "Admin user created."

# /api/setup's inline "database" field is unreliable on this version, so the
# DW connection is added separately via /api/database using the session it returns.
SESSION_ID=$(jq -r '.id' /tmp/setup_response.json)

echo "Connecting postgres_dw..."

DB_PAYLOAD=$(jq -n \
  '{
    engine: "postgres",
    name: "postgres_dw",
    details: {
      host: "postgres_dw",
      port: 5432,
      dbname: "tfl_dw",
      user: "dw_user",
      password: "dw_password",
      ssl: false,
      "tunnel-enabled": false
    },
    is_full_sync: true
  }')

DB_HTTP_STATUS=$(curl -s -o /tmp/db_response.json -w "%{http_code}" -X POST "$METABASE_URL/api/database" \
  -H "Content-Type: application/json" \
  -H "X-Metabase-Session: $SESSION_ID" \
  -d "$DB_PAYLOAD")

if [ "$DB_HTTP_STATUS" = "200" ]; then
  echo "postgres_dw connected successfully."
else
  echo "Adding postgres_dw failed with HTTP status $DB_HTTP_STATUS"
  cat /tmp/db_response.json
  exit 1
fi
