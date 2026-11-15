FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY prihvat.py .

EXPOSE 8000
CMD ["uvicorn", "prihvat:app", "--host", "0.0.0.0", "--port", "8000"]
