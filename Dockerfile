# --- Stage 1: Build ---
FROM python:3.12-slim AS builder

WORKDIR /app

COPY requirements.txt ./
RUN pip install --user --no-cache-dir -r requirements.txt

# --- Stage 2: Runtime ---
FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /root/.local /root/.local

ENV PATH=/root/.local/bin:$PATH

COPY . .

CMD ["python", "etl.py"]
