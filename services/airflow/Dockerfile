FROM apache/airflow:2.9.0-python3.11
RUN pip install --no-cache-dir boto3 pandas pyarrow psycopg2-binary SQLAlchemy fastembed qdrant-client
COPY dags/ /opt/airflow/dags/
