
# save as upload_parquet.py
import os
import io
import uuid
from datetime import date

import pandas as pd
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# ----- USER CONFIG -----
BUCKET = "Replace with bucket name"  # e.g., "my-analytics-bucket"
DATASET_PREFIX = "events"  # top-level folder for the dataset
AWS_REGION = "us-east-1"   # pick your region
KMS_KEY_ID = "Replace with KMS ID"  # e.g., "arn:aws:kms:us-east-1:123456789012:key/xxxx-..."
AWS_PROFILE = os.getenv("AWS_PROFILE", "MY_SSO_PROFILE")  # set your profile or leave blank if running in AWS
# -----------------------

def get_s3_client():
    cfg = Config(
        region_name=AWS_REGION,
        retries={"max_attempts": 10, "mode": "standard"},
        s3={"addressing_style": "virtual"},
        connect_timeout=5,
        read_timeout=120,
    )
    if AWS_PROFILE:
        session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    else:
        session = boto3.Session(region_name=AWS_REGION)
    return session.client("s3", config=cfg)

def make_sample_dataframe():
    # Replace this with your real data transform
    return pd.DataFrame(
        {
            "event_time": pd.to_datetime(
                ["2026-01-13T10:00:00Z", "2026-01-13T10:05:00Z", "2026-01-13T10:10:00Z"]
            ),
            "user_id": [101, 102, 103],
            "event_type": ["login", "click", "purchase"],
            "amount": [0.0, 0.0, 19.99],
        }
    )

def dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    # Parquet best practices: snappy compression; no index
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
    buf.seek(0)
    return buf.getvalue()

def upload_parquet_bytes(s3, body: bytes, bucket: str, key: str):
    # Put directly uses one shot; for >100MB consider upload_file with a temp file
    extra = {
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": KMS_KEY_ID,
        "ChecksumAlgorithm": "CRC32C",
        "ContentType": "application/octet-stream",
    }
    s3.put_object(Bucket=bucket, Key=key, Body=body, **extra)
    print(f"Uploaded: s3://{bucket}/{key}")

def safe_finalize_pattern(s3, tmp_key: str, final_key: str, bucket: str):
    """Copy from staging to final atomically(ish), then delete staging."""
    extra = {
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": KMS_KEY_ID,
    }
    s3.copy(
        CopySource={"Bucket": bucket, "Key": tmp_key},
        Bucket=bucket,
        Key=final_key,
        ExtraArgs=extra,
    )
    s3.delete_object(Bucket=bucket, Key=tmp_key)
    print(f"Finalized: s3://{bucket}/{final_key}")

def main():
    s3 = get_s3_client()

    # Build partition path based on today's date
    partition_dt = date.today().isoformat()  # e.g., 2026-01-13
    partition_prefix = f"{DATASET_PREFIX}/dt={partition_dt}"

    # Create DataFrame and serialize to Parquet
    df = make_sample_dataframe()
    body = dataframe_to_parquet_bytes(df)

    # Idempotent write: upload to staging first, then copy to final
    staging_key = f"{partition_prefix}/staging/{uuid.uuid4()}.parquet"
    final_key = f"{partition_prefix}/part-0000.parquet"

    try:
        upload_parquet_bytes(s3, body, BUCKET, staging_key)
        safe_finalize_pattern(s3, staging_key, final_key, BUCKET)
    except ClientError as e:
        print(f"ERROR: {e}")
        # In real code, emit metrics/logs/alerts here
        raise

if __name__ == "__main__":
    main()
