FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python boto3 pandas pyarrow
COPY bronze_worker.py .
CMD ["python", "-u", "bronze_worker.py"]
