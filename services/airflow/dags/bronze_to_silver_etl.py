import io
import uuid
from datetime import datetime, timedelta

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.client import Config
from fastembed import TextEmbedding
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams
from sqlalchemy import create_engine

BUCKET = "bronze-logs"
COLLECTION = "system_logs"


import os

def bronze_to_silver():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio.lakehouse.svc.cluster.local:9000",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )
    db_uri = os.getenv("POSTGRES_URI", "postgresql://agent:agentpassword@postgres.lakehouse.svc.cluster.local:5432/analytical_db")
    postgres = create_engine(db_uri)
    qdrant = QdrantClient(url="http://qdrant.lakehouse.svc.cluster.local:6333", timeout=60)
    embedder = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")

    if not qdrant.collection_exists(COLLECTION):
        qdrant.create_collection(COLLECTION, vectors_config=VectorParams(size=384, distance=Distance.COSINE))

    for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix="ingested/").get("Contents", []):
        key = obj["Key"]
        if not key.endswith(".parquet"):
            continue

        df = pd.read_parquet(io.BytesIO(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()))
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.to_sql("system_events", postgres, if_exists="append", index=False)

        texts = (df["level"] + " at " + df["timestamp"].astype(str) + ": " + df["message"]).tolist()
        points = [
            PointStruct(
                id=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{key}_{i}")),
                vector=vector.tolist(),
                payload={
                    "service": row["service"],
                    "level": row["level"],
                    "message": row["message"],
                    "timestamp": str(row["timestamp"]),
                },
            )
            for (i, row), vector in zip(df.iterrows(), embedder.embed(texts))
        ]
        qdrant.upsert(COLLECTION, points)

        s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": key}, Key=key.replace("ingested/", "archive/"))
        s3.delete_object(Bucket=BUCKET, Key=key)


with DAG(
    "bronze_to_silver_etl",
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
) as dag:
    PythonOperator(task_id="process_parquet_to_postgres", python_callable=bronze_to_silver)
