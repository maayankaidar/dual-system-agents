FROM python:3.11-slim
WORKDIR /app
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir kafka-python boto3 pandas pyarrow
COPY bronze_worker.py .
CMD ["python", "-u", "bronze_worker.py"]
