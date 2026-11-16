FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# 1 worker + threads: tiny service; the in-memory cache is shared by all requests
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 60 app:app
