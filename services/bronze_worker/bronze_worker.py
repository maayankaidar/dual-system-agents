import io
import json
import os
from datetime import datetime

import boto3
import pandas as pd
from botocore.client import Config
from botocore.exceptions import ClientError
from kafka import KafkaConsumer

BUCKET = "bronze-logs"
BATCH_SIZE = 50

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["MINIO_ENDPOINT"],
    aws_access_key_id=os.environ["MINIO_ROOT_USER"],
    aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    region_name="us-east-1",
)

try:
    s3.head_bucket(Bucket=BUCKET)
except ClientError:
    s3.create_bucket(Bucket=BUCKET)

consumer = KafkaConsumer(
    "system-logs",
    bootstrap_servers=os.environ["KAFKA_BROKER"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id="bronze-ingestion-group",
    auto_offset_reset="earliest",
)

batch = []
for message in consumer:
    batch.append(message.value)
    if len(batch) == BATCH_SIZE:
        buffer = io.BytesIO()
        pd.DataFrame(batch).to_parquet(buffer)
        key = f"ingested/bronze_batch_{datetime.now():%Y%m%d_%H%M%S}.parquet"
        s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
        print(f"Uploaded {BATCH_SIZE} logs to s3://{BUCKET}/{key}", flush=True)
        batch = []
