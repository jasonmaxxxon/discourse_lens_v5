FROM mcr.microsoft.com/playwright/python:v1.57.0-jammy

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# Ensure local modules resolve (e.g., scraper/)
ENV PYTHONPATH=/app

# Default: run fetch + ingest; URL provided via env.
CMD ["bash", "-lc", "python3 scripts/run_fetcher_and_ingest.py \"$URL\" --headless"]
