"""
Kafka Producer - Online Retail II Streaming Simulator
======================================================
Excel dosyasından (online_retail_II.xlsx) satır satır okuyup, JSON formatında
Kafka topic'ine gönderir. Gerçek zamanlı veri akışını simüle eder.

Hocanın istediği zorunlu alanlar:
- timestamp: Mesajın gönderildiği an (ISO format)
- kullanici_ID: CustomerID
- olay_tipi: 'purchase' veya 'cancellation'
- ilgili_ID: InvoiceNo

Ek alanlar (zenginleştirilmiş veri):
- stock_code, description, quantity, unit_price, country, invoice_date

Yapılandırma (.env veya çevre değişkenleri):
- KAFKA_BOOTSTRAP_SERVERS (default: kafka:9092)
- KAFKA_TOPIC            (default: online-retail-transactions)
- PRODUCER_RATE_PER_SECOND (default: 50)
- DATA_PATH              (default: /app/data/raw/online_retail_II.xlsx)
- MAX_MESSAGES           (default: 0 = sınırsız)
"""
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


# =============================================================================
# YAPILANDIRMA
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "online-retail-transactions")
RATE_PER_SECOND = int(os.getenv("PRODUCER_RATE_PER_SECOND", "50"))
DATA_PATH = os.getenv("DATA_PATH", "/app/data/raw/online_retail_II.xlsx")
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "0"))  # 0 = sınırsız

LOG_EVERY = 1000  # her N mesajda bir log basılır

# =============================================================================
# LOG YAPILANDIRMASI
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("kafka-producer")


# =============================================================================
# GRACEFUL SHUTDOWN (Ctrl+C ile düzgün kapatma)
# =============================================================================
class GracefulShutdown:
    def __init__(self):
        self.shutdown = False
        signal.signal(signal.SIGINT, self._handler)
        signal.signal(signal.SIGTERM, self._handler)

    def _handler(self, signum, frame):
        logger.info("⚠️  Kapatma sinyali alındı. Mevcut mesaj bittikten sonra kapatılacak...")
        self.shutdown = True


# =============================================================================
# YARDIMCI FONKSİYONLAR
# =============================================================================
def load_data(path: str) -> pd.DataFrame:
    """Excel'in iki sayfasını okur, birleştirir ve tarihe göre sıralar."""
    file_path = Path(path)
    if not file_path.exists():
        logger.error(f"❌ Veri dosyası bulunamadı: {path}")
        raise FileNotFoundError(f"Veri dosyası yok: {path}")

    logger.info(f"📂 Veri yükleniyor: {path}")

    df_2009 = pd.read_excel(path, sheet_name="Year 2009-2010")
    logger.info(f"   ↳ 2009-2010: {len(df_2009):,} satır")

    df_2010 = pd.read_excel(path, sheet_name="Year 2010-2011")
    logger.info(f"   ↳ 2010-2011: {len(df_2010):,} satır")

    df = pd.concat([df_2009, df_2010], ignore_index=True)

    # Sütun isimlerini standartlaştır (UCI bazen farklı isimlendiriyor)
    df.columns = ["InvoiceNo", "StockCode", "Description", "Quantity",
                  "InvoiceDate", "UnitPrice", "CustomerID", "Country"]

    # Tarihe göre sırala (gerçek zamanlı simülasyon için)
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df = df.sort_values("InvoiceDate").reset_index(drop=True)

    logger.info(f"✅ Toplam: {len(df):,} satır, sıralandı")
    return df


def row_to_message(row: pd.Series) -> dict:
    """
    Bir DataFrame satırını Kafka için JSON mesajına dönüştürür.
    Hocanın istediği zorunlu alanlar: timestamp, kullanici_ID, olay_tipi, ilgili_ID
    """
    invoice_no = str(row["InvoiceNo"])
    olay_tipi = "cancellation" if invoice_no.startswith("C") else "purchase"

    # CustomerID NaN olabilir (~%20 satırda)
    customer_id = row["CustomerID"]
    customer_id = int(customer_id) if pd.notna(customer_id) else None

    # Description NaN olabilir
    description = row["Description"]
    description = str(description) if pd.notna(description) else None

    return {
        # === Hocanın istediği zorunlu alanlar ===
        "timestamp": datetime.utcnow().isoformat(),
        "kullanici_ID": customer_id,
        "olay_tipi": olay_tipi,
        "ilgili_ID": invoice_no,

        # === Ek alanlar (zenginleştirme) ===
        "stock_code": str(row["StockCode"]),
        "description": description,
        "quantity": int(row["Quantity"]),
        "unit_price": float(row["UnitPrice"]),
        "country": str(row["Country"]),
        "invoice_date": row["InvoiceDate"].isoformat()
    }


def create_producer(bootstrap_servers: str, retries: int = 10, retry_delay: int = 5) -> KafkaProducer:
    """Kafka producer oluşturur, broker hazır olana kadar yeniden dener."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                acks="all",                      # tüm replica'lar onaylasın
                retries=5,                       # gönderim hatasında otomatik retry
                linger_ms=10,                    # batch oluşturmak için 10ms bekle
                compression_type="gzip"          # mesajları sıkıştır
            )
            logger.info(f"✅ Kafka'ya bağlanıldı: {bootstrap_servers}")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"⏳ Kafka henüz hazır değil, deneme {attempt}/{retries}, {retry_delay}s bekleniyor...")
            time.sleep(retry_delay)

    raise RuntimeError(f"❌ Kafka'ya {retries} denemede bağlanılamadı: {bootstrap_servers}")


# =============================================================================
# ANA İŞ MANTIĞI
# =============================================================================
def main():
    logger.info("=" * 70)
    logger.info("🚀 Kafka Producer Başlatılıyor")
    logger.info("=" * 70)
    logger.info(f"   Kafka:        {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"   Topic:        {KAFKA_TOPIC}")
    logger.info(f"   Hız:          {RATE_PER_SECOND} mesaj/saniye")
    logger.info(f"   Veri dosyası: {DATA_PATH}")
    if MAX_MESSAGES > 0:
        logger.info(f"   Max mesaj:    {MAX_MESSAGES}")
    else:
        logger.info(f"   Max mesaj:    sınırsız")
    logger.info("=" * 70)

    shutdown = GracefulShutdown()

    # Veri yükle
    df = load_data(DATA_PATH)

    # Producer oluştur (Kafka hazır olana kadar bekle)
    producer = create_producer(KAFKA_BOOTSTRAP_SERVERS)

    # Hıza göre saniye başına bekleme süresi
    sleep_per_message = 1.0 / RATE_PER_SECOND if RATE_PER_SECOND > 0 else 0

    # Sayaçlar
    sent = 0
    errors = 0
    start_time = time.time()
    last_log_time = start_time

    try:
        for idx, row in df.iterrows():
            if shutdown.shutdown:
                logger.info("⚠️  Shutdown talebi geldi, döngü kesiliyor.")
                break

            if MAX_MESSAGES > 0 and sent >= MAX_MESSAGES:
                logger.info(f"🎯 Hedef mesaj sayısına ulaşıldı: {MAX_MESSAGES}")
                break

            try:
                message = row_to_message(row)
                # CustomerID partition key olarak kullanılır (aynı müşteri aynı partition'a)
                key = message.get("kullanici_ID")

                producer.send(KAFKA_TOPIC, key=key, value=message)
                sent += 1

                # Periyodik log
                if sent % LOG_EVERY == 0:
                    now = time.time()
                    elapsed = now - last_log_time
                    rate = LOG_EVERY / elapsed if elapsed > 0 else 0
                    total_elapsed = now - start_time
                    overall_rate = sent / total_elapsed if total_elapsed > 0 else 0
                    logger.info(
                        f"📤 Gönderildi: {sent:,} | "
                        f"Anlık hız: {rate:.1f} msg/s | "
                        f"Ortalama: {overall_rate:.1f} msg/s | "
                        f"Hata: {errors}"
                    )
                    last_log_time = now

                # Hız kontrolü
                if sleep_per_message > 0:
                    time.sleep(sleep_per_message)

            except KafkaError as e:
                errors += 1
                logger.error(f"⚠️  Mesaj gönderilemedi (idx={idx}): {e}")
                continue

    finally:
        logger.info("🔄 Producer flush ediliyor (kalan mesajlar gönderiliyor)...")
        producer.flush(timeout=30)
        producer.close(timeout=10)

        total_elapsed = time.time() - start_time
        overall_rate = sent / total_elapsed if total_elapsed > 0 else 0

        logger.info("=" * 70)
        logger.info("📊 ÖZET")
        logger.info("=" * 70)
        logger.info(f"   Toplam gönderilen: {sent:,} mesaj")
        logger.info(f"   Toplam hata:        {errors}")
        logger.info(f"   Toplam süre:        {total_elapsed:.1f} saniye")
        logger.info(f"   Ortalama hız:       {overall_rate:.1f} msg/s")
        logger.info("=" * 70)
        logger.info("✅ Producer kapatıldı.")


if __name__ == "__main__":
    main()