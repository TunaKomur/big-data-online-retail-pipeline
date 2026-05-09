FROM python:3.10-slim

# Çalışma dizini
WORKDIR /app

# Sistem bağımlılıkları
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Python paketleri
COPY docker/producer/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Producer kodu volume mount ile gelecek (geliştirme kolaylığı)
# Container ayakta kalır, kod docker exec ile çalıştırılır

# Default command - container ayakta kalsın
CMD ["tail", "-f", "/dev/null"]