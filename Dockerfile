FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV DATA_DIR=/data

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000
CMD ["sh", "-c", "gunicorn --workers 1 --threads 8 --timeout 300 --bind 0.0.0.0:${PORT:-5000} app:app"]
