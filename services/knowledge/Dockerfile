FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY knowledge_service ./knowledge_service

RUN pip install --no-cache-dir .

CMD ["python", "-m", "knowledge_service"]

