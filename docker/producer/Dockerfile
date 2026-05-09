FROM python:3.10-slim

# Çalışma dizini
WORKDIR /app

# Sistem bağımlılıkları (excel okumak için minimum)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Python paketleri
COPY docker/producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Producer kodu sonradan volume mount edilecek
# (geliştirme aşamasında değiştirip rebuild etmek istemiyoruz)

# Default command - Faz 3'te güncellenecek
CMD ["python", "-c", "print('Producer container ready. Run kafka_producer.py manually or via docker exec.')"]