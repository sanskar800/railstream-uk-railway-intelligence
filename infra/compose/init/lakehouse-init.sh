#!/bin/sh
# Idempotent lakehouse setup, run once per `make up`:
#   1. the S3 bucket for the Iceberg warehouse;
#   2. Lakekeeper bootstrap (first start only);
#   3. the `railstream` warehouse pointing at that bucket.
# Each step checks first, so re-running is safe.
set -eu

BUCKET=warehouse
WAREHOUSE=railstream
PROJECT_ID=00000000-0000-0000-0000-000000000000
CATALOG=http://lakekeeper:8181

echo "== bucket '$BUCKET'"
if echo "s3.bucket.list" | weed shell -master=seaweedfs:9333 2>/dev/null | grep -qw "$BUCKET"; then
  echo "exists"
else
  echo "s3.bucket.create -name $BUCKET" | weed shell -master=seaweedfs:9333
fi

echo "== catalog bootstrap"
if wget -qO- "$CATALOG/management/v1/info" | grep -q '"bootstrapped":true'; then
  echo "already bootstrapped"
else
  wget -qO- --header 'Content-Type: application/json' \
    --post-data '{"accept-terms-of-use": true}' "$CATALOG/management/v1/bootstrap"
  echo "bootstrapped"
fi

echo "== warehouse '$WAREHOUSE'"
if wget -qO- --header "x-project-id: $PROJECT_ID" "$CATALOG/management/v1/warehouse" \
    | grep -q "\"name\":\"$WAREHOUSE\""; then
  echo "exists"
else
  # Lakekeeper validates the storage profile by writing and deleting a test object.
  # SeaweedFS has no STS, so clients use their own S3 credentials (sts-enabled false).
  wget -qO- --header 'Content-Type: application/json' --post-data "{
    \"warehouse-name\": \"$WAREHOUSE\",
    \"project-id\": \"$PROJECT_ID\",
    \"storage-profile\": {
      \"type\": \"s3\",
      \"bucket\": \"$BUCKET\",
      \"key-prefix\": \"$WAREHOUSE\",
      \"endpoint\": \"http://seaweedfs:8333\",
      \"region\": \"us-east-1\",
      \"path-style-access\": true,
      \"flavor\": \"s3-compat\",
      \"sts-enabled\": false
    },
    \"storage-credential\": {
      \"type\": \"s3\",
      \"credential-type\": \"access-key\",
      \"aws-access-key-id\": \"$S3_ACCESS_KEY\",
      \"aws-secret-access-key\": \"$S3_SECRET_KEY\"
    }
  }" "$CATALOG/management/v1/warehouse"
  echo
  echo "created"
fi
