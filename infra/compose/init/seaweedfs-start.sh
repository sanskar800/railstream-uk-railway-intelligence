#!/bin/sh
# Start SeaweedFS in single-container mode (master + volume + filer + S3 gateway).
# The S3 identity is written from environment variables so no credentials live in the repo.
set -eu

cat > /tmp/s3.json <<EOF
{
  "identities": [
    {
      "name": "railstream",
      "credentials": [{"accessKey": "${S3_ACCESS_KEY}", "secretKey": "${S3_SECRET_KEY}"}],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ]
}
EOF

exec weed server -dir=/data -ip.bind=0.0.0.0 \
  -master.volumeSizeLimitMB=1024 -volume.max=0 \
  -s3 -s3.port=8333 -s3.config=/tmp/s3.json
