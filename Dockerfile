FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir requests feedparser pynostr keyring prompt_toolkit textual

COPY peertube_nostr.py /app/peertube_nostr.py

ENTRYPOINT ["python", "/app/peertube_nostr.py"]
