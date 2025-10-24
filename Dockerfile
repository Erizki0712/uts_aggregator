FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN adduser --disabled-password --gecos '' appuser && chown -R appuser:appuser /app

COPY src/ ./src/
COPY tests/ ./tests/

EXPOSE 8080

CMD ["python", "-m", "src.main"]