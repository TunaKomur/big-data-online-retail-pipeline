"""
Silver Transformation Job
=========================
Bronze tablosundan oku, temizle, Silver'a yaz.

Çalıştırma:
    python spark_jobs/silver_transformation.py
"""
import logging
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.functions import col, to_timestamp
from src.spark_session import get_spark_session


BRONZE_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "delta_lake", "bronze", "transactions")
)
SILVER_PURCHASES_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "delta_lake", "silver", "transactions")
)
SILVER_CANCELLATIONS_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "delta_lake", "silver", "cancellations")
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("silver")


def main():
    logger.info("🥈 Silver Transformation başladı")

    spark = get_spark_session("Silver-Transformation", driver_memory="3g")

    # Bronze oku
    df_bronze = spark.read.format("delta").load(BRONZE_PATH)
    logger.info(f"📊 Bronze: {df_bronze.count():,} satır")

    # Tip dönüşümü ve türetilmiş alanlar
    df_typed = df_bronze \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("invoice_date", to_timestamp(col("invoice_date"))) \
        .withColumn("total_price", col("quantity") * col("unit_price")) \
        .withColumn("is_cancellation", col("olay_tipi") == "cancellation")

    # Temizlik
    df_clean = df_typed \
        .filter(col("kullanici_ID").isNotNull()) \
        .filter(col("description").isNotNull()) \
        .dropDuplicates(["ilgili_ID", "stock_code", "kullanici_ID", "invoice_date", "quantity"])

    logger.info(f"📊 Temizlenmiş: {df_clean.count():,} satır")

    # Purchase / cancellation ayır
    df_purchases = df_clean.filter(~col("is_cancellation")) \
                           .filter(col("quantity") > 0) \
                           .filter(col("unit_price") > 0)
    df_cancellations = df_clean.filter(col("is_cancellation"))

    purch_count = df_purchases.count()
    canc_count = df_cancellations.count()

    logger.info(f"🛒 Purchases:     {purch_count:,}")
    logger.info(f"❌ Cancellations: {canc_count:,}")

    # Yaz
    df_purchases.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(SILVER_PURCHASES_PATH)

    df_cancellations.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(SILVER_CANCELLATIONS_PATH)

    logger.info("✅ Silver tabloları yazıldı")
    spark.stop()


if __name__ == "__main__":
    main()