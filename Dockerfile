FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY ai_advokat_parser ./ai_advokat_parser

RUN pip install --no-cache-dir .

CMD ["python", "-m", "ai_advokat_parser.railway_worker"]
