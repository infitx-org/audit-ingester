# export REQUIRE_CONFIRMATION=false
# export DRY_RUN=false
# python3 s3_purge.py

import os
import sys
import logging
import boto3
import urllib3
from dotenv import load_dotenv

# ---------------------------------------------------
# Environment & Logging
# ---------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------------------------------------------------
# Config
# ---------------------------------------------------

S3_BUCKET = os.getenv("S3_BUCKET")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_PREFIX = os.getenv("S3_FOLDER")  # optional
S3_SKIP_SSL_VERIFY = os.getenv("S3_SKIP_SSL_VERIFY", "false").lower() == "true"

# Safety flags
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
REQUIRE_CONFIRMATION = os.getenv("REQUIRE_CONFIRMATION", "true").lower() == "true"

if not S3_BUCKET:
    logging.error("S3_BUCKET must be set")
    sys.exit(1)

# ---------------------------------------------------
# SSL Warning Handling
# ---------------------------------------------------

if S3_SKIP_SSL_VERIFY:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------
# S3 Client
# ---------------------------------------------------

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    verify=not S3_SKIP_SSL_VERIFY,
)

# ---------------------------------------------------
# Helpers
# ---------------------------------------------------

def normalize_prefix(prefix):
    if not prefix:
        return None
    return prefix.rstrip("/") + "/"

PREFIX = normalize_prefix(S3_PREFIX)

def bucket_is_versioned():
    resp = s3.get_bucket_versioning(Bucket=S3_BUCKET)
    return resp.get("Status") == "Enabled"

def count_objects():
    paginator = s3.get_paginator("list_objects_v2")
    kwargs = {"Bucket": S3_BUCKET}
    if PREFIX:
        kwargs["Prefix"] = PREFIX

    count = 0
    for page in paginator.paginate(**kwargs):
        count += len(page.get("Contents", []))
    return count

def count_versions():
    paginator = s3.get_paginator("list_object_versions")
    kwargs = {"Bucket": S3_BUCKET}
    if PREFIX:
        kwargs["Prefix"] = PREFIX

    count = 0
    for page in paginator.paginate(**kwargs):
        count += len(page.get("Versions", []))
        count += len(page.get("DeleteMarkers", []))
    return count

# ---------------------------------------------------
# Delete Logic
# ---------------------------------------------------

def delete_non_versioned():
    paginator = s3.get_paginator("list_objects_v2")
    kwargs = {"Bucket": S3_BUCKET}
    if PREFIX:
        kwargs["Prefix"] = PREFIX

    deleted = 0

    for page in paginator.paginate(**kwargs):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]

        for i in range(0, len(keys), 1000):
            chunk = keys[i:i + 1000]

            if DRY_RUN:
                for k in chunk:
                    logging.info(f"[DRY-RUN] Would delete {k['Key']}")
                continue

            s3.delete_objects(
                Bucket=S3_BUCKET,
                Delete={"Objects": chunk, "Quiet": True},
            )
            deleted += len(chunk)
            logging.info(f"Deleted {len(chunk)} objects")

    return deleted

def delete_versioned():
    paginator = s3.get_paginator("list_object_versions")
    kwargs = {"Bucket": S3_BUCKET}
    if PREFIX:
        kwargs["Prefix"] = PREFIX

    deleted = 0

    for page in paginator.paginate(**kwargs):
        keys = []

        for v in page.get("Versions", []):
            keys.append({"Key": v["Key"], "VersionId": v["VersionId"]})

        for m in page.get("DeleteMarkers", []):
            keys.append({"Key": m["Key"], "VersionId": m["VersionId"]})

        for i in range(0, len(keys), 1000):
            chunk = keys[i:i + 1000]

            if DRY_RUN:
                for k in chunk:
                    logging.info(
                        f"[DRY-RUN] Would delete {k['Key']} (version {k['VersionId']})"
                    )
                continue

            s3.delete_objects(
                Bucket=S3_BUCKET,
                Delete={"Objects": chunk, "Quiet": True},
            )
            deleted += len(chunk)
            logging.info(f"Deleted {len(chunk)} object versions")

    return deleted

# ---------------------------------------------------
# Main
# ---------------------------------------------------

if __name__ == "__main__":

    target = f"{S3_BUCKET}/{PREFIX or ''}"
    logging.warning(f"TARGET: {target}")

    versioned = bucket_is_versioned()
    logging.info(f"Bucket versioning: {'ENABLED' if versioned else 'DISABLED'}")

    visible_objects = count_objects()
    version_count = count_versions() if versioned else visible_objects

    logging.info(f"Visible objects: {visible_objects}")
    logging.info(f"Total versions: {version_count}")

    if visible_objects == 0 and version_count == 0:
        logging.warning("Nothing to delete")
        sys.exit(0)

    if REQUIRE_CONFIRMATION and not DRY_RUN:
        confirm = input("Type DELETE to continue: ")
        if confirm != "DELETE":
            logging.info("Aborted")
            sys.exit(0)

    if DRY_RUN:
        logging.info("DRY-RUN mode enabled — no deletes will occur")

    deleted = (
        delete_versioned()
        if versioned
        else delete_non_versioned()
    )

    logging.info(f"TOTAL DELETED: {deleted}")
