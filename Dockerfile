FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi uvicorn httpx
COPY config.py common.py worker.py coordinator.py ./
ENV DATA_PATH=/data/sample.csv PORT=8000
CMD ["python", "worker.py"]
