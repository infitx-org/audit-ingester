# audit-ingester
Ingest audit messages

## Removing files in the s3 bucket - Useful commands

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test123
export S3_ENDPOINT_URL=https://cephobjectstore.storage.cc.domain.com
s5cmd --no-verify-ssl rm "s3://audit-region-dev-drpp-onprem-global/orc/*"