FROM python:3.11-slim
WORKDIR /app
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir kafka-python faker
COPY producer.py .
CMD ["python", "-u", "producer.py"]
