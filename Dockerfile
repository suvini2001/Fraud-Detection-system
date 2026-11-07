# One image for every service (api, producer, scorer, alerts);
# docker-compose chooses what to run with `command:`.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir --retries 10 --timeout 120 -r requirements.txt

COPY app/ app/

# run as non-root
RUN useradd -r -u 10001 appuser && mkdir -p /data && chown appuser /data
USER appuser

CMD ["python", "-u", "-m", "app.consumer"]