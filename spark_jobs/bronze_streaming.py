"""
Bronze Streaming Job
====================
Kafka'dan streaming veri okur, Delta Lake Bronze tablosuna yazar.

Çalıştırma:
    python spark_jobs/bronze_streaming.py
    
    veya kontrollü süre ile:
    python spark_jobs/bronze_streaming.py --duration 60

Durdurma:
    Ctrl+C (graceful shutdown)
"""
import argparse
import logging
import os
import sys
import time

# Proje kök dizinini path'e ekle
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, LongType
)

from src.spark_session import get_spark_session


# Yapılandırma
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "online-retail-transactions")
BRONZE_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "delta_lake", "bronze", "transactions")
)
CHECKPOINT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "delta_lake", "_checkpoints", "bronze_transactions")
)

# Şema
MESSAGE_SCHEMA = StructType([
    StructField("timestamp", StringType(), True),
    StructField("kullanici_ID", LongType(), True),
    StructField("olay_tipi", StringType(), True),
    StructField("ilgili_ID", StringType(), True),
    StructField("stock_code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("invoice_date", StringType(), True),
])

# Log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("bronze-streaming")


def main(duration_seconds: int = 0):
    logger.info("=" * 70)
    logger.info("🥉 Bronze Streaming Job Başlatılıyor")
    logger.info("=" * 70)
    logger.info(f"   Kafka:       {KAFKA_BOOTSTRAP}")
    logger.info(f"   Topic:       {KAFKA_TOPIC}")
    logger.info(f"   Bronze path: {BRONZE_PATH}")
    logger.info(f"   Checkpoint:  {CHECKPOINT_PATH}")
    if duration_seconds > 0:
        logger.info(f"   Süre limiti:  {duration_seconds} saniye")
    logger.info("=" * 70)

    spark = get_spark_session(
        app_name="Bronze-Streaming-Job",
        driver_memory="3g"
    )

    # Kafka'dan oku
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # JSON parse + metadata
    df_parsed = df_kafka.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), MESSAGE_SCHEMA).alias("data")
    ).select(
        "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        "data.*",
        current_timestamp().alias("ingestion_time")
    )

    # Delta'ya yaz
    query = df_parsed.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .option("path", BRONZE_PATH) \
        .trigger(processingTime="5 seconds") \
        .start()

    logger.info(f"✅ Stream başladı, ID: {query.id}")

    try:
        if duration_seconds > 0:
            time.sleep(duration_seconds)
            logger.info(f"⏱️  Süre limiti ({duration_seconds}s) doldu, stream durduruluyor...")
        else:
            query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("⚠️  Ctrl+C alındı, graceful shutdown...")
    finally:
        query.stop()
        logger.info("✅ Stream durduruldu")

        # Final stat
        try:
            df_bronze = spark.read.format("delta").load(BRONZE_PATH)
            total = df_bronze.count()
            logger.info(f"📊 Bronze tabloda toplam: {total:,} satır")
        except Exception as e:
            logger.warning(f"İstatistik alınamadı: {e}")

        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=0,
                        help="Saniye olarak çalışma süresi (0=sınırsız)")
    args = parser.parse_args()
    main(duration_seconds=args.duration)