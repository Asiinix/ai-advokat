FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN useradd --system --uid 10001 --create-home egressproxy

WORKDIR /app
COPY --chown=egressproxy:egressproxy server.py /app/server.py

USER 10001

CMD ["python", "/app/server.py"]
