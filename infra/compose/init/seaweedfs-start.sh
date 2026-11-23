#!/bin/sh
# Start SeaweedFS in single-container mode (master + volume + filer + S3 gateway).
# S3 identities are written from environment variables so no credentials live in the repo:
# `railstream` (Lakekeeper, which signs requests for every other client) and `dbt` (DuckDB cannot
# use remote signing, so it gets its own key, limited to the warehouse bucket; ADR-017).
set -eu

cat > /tmp/s3.json <<EOF
{
  "identities": [
    {
      "name": "railstream",
      "credentials": [{"accessKey": "${S3_ACCESS_KEY}", "secretKey": "${S3_SECRET_KEY}"}],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    },
    {
      "name": "dbt",
      "credentials": [{"accessKey": "${DBT_S3_ACCESS_KEY}", "secretKey": "${DBT_S3_SECRET_KEY}"}],
      "actions": ["Read:warehouse", "Write:warehouse", "List:warehouse"]
    }
  ]
}
EOF

exec weed server -dir=/data -ip.bind=0.0.0.0 \
  -master.volumeSizeLimitMB=1024 -volume.max=0 \
  -s3 -s3.port=8333 -s3.config=/tmp/s3.json
