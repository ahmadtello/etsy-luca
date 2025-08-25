# ./Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Europe/Istanbul \
    DB_PATH=/data/app.sqlite3

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends tzdata curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY templates ./templates
COPY main.py .

# Non-root user + persistent data dir
RUN useradd -ms /bin/bash appuser && mkdir -p /data && chown -R appuser:appuser /app /data
USER appuser

EXPOSE 8000
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8000","--proxy-headers"]
