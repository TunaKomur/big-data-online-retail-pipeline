# Kafka Producer

Online Retail II veri setinden Kafka topic'ine gerçek zamanlı veri akışı simüle eder.

## 📋 Mesaj Formatı

```json
{
  "timestamp": "2026-05-09T18:42:13.123456",
  "kullanici_ID": 17850,
  "olay_tipi": "purchase",
  "ilgili_ID": "536365",
  "stock_code": "85123A",
  "description": "WHITE HANGING HEART T-LIGHT HOLDER",
  "quantity": 6,
  "unit_price": 2.55,
  "country": "United Kingdom",
  "invoice_date": "2010-12-01T08:26:00"
}
```

## 🚀 Kullanım

### Container içinde çalıştır (önerilen)

```bash
# Tüm dataset
docker exec -it producer python /app/kafka_producer.py

# Sadece 1000 mesaj (test için)
docker exec -e MAX_MESSAGES=1000 -it producer python /app/kafka_producer.py

# Hızı değiştirerek
docker exec -e PRODUCER_RATE_PER_SECOND=100 -it producer python /app/kafka_producer.py
```

### Lokal (host) test (sanal ortam aktifken)

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9094 \
DATA_PATH=./data/raw/online_retail_II.xlsx \
MAX_MESSAGES=100 \
python producer/kafka_producer.py
```

## ⚙️ Çevre Değişkenleri

| Değişken | Default | Açıklama |
|----------|---------|----------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker adresi |
| `KAFKA_TOPIC` | `online-retail-transactions` | Hedef topic |
| `PRODUCER_RATE_PER_SECOND` | `50` | Saniyede gönderilecek mesaj sayısı |
| `DATA_PATH` | `/app/data/raw/online_retail_II.xlsx` | Excel dosyasının yolu |
| `MAX_MESSAGES` | `0` (sınırsız) | Maksimum gönderilecek mesaj |

## 🛑 Durdurma

`Ctrl+C` ile durdur — graceful shutdown ile kalan mesajlar gönderilir.