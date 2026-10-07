FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY chat_ui.py .
CMD ["streamlit", "run", "chat_ui.py", "--server.port=8501", "--server.address=0.0.0.0"]

