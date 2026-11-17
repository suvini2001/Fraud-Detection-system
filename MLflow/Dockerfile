FROM python:3.11-slim

WORKDIR /mlflow

# Testing අවසානයේ exact version එකට pin කරන්න (උදා: mlflow==3.x.y)
RUN pip install --no-cache-dir "mlflow>=3.10,<4"

RUN mkdir -p /mlflow/data/artifacts

EXPOSE 5000

# slim image එකේ curl නැති නිසා Python වලින් /health check කරනවා
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/health', timeout=3)" || exit 1