FROM python:3.12-slim

# Ispis odmah u log (bez međuspremnika), da se u "docker compose logs" vidi uživo
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sonda.py .

CMD ["python", "sonda.py"]
