FROM python:3.10-slim

RUN apt-get update && apt-get install -y curl ffmpeg && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY worker.py ./worker.py

# v-worker API: Flask/Gunicorn (video pipeline, I/O-bound com servicos externos)
# 4 workers x 4 threads = 16 slots (suficiente para admin/orchestrator)
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "4", "--threads", "4", "--timeout", "120", "--worker-class", "gthread", "app.main:app"]
