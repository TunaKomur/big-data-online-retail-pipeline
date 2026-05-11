"""
Gold Feature Engineering Job
=============================
Silver tablolarından müşteri seviyesinde feature tablosu (Gold) oluşturur.

Çalıştırma:
    python spark_jobs/gold_feature_engineering.py
"""
import logging
import os
import sys
from datetime import timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.functions import (
    col, lit, datediff, countDistinct, count, sum as _sum,
    min as _min, max as _max, when, date_format, hour, row_number
)
from pyspark.sql.window import Window

from src.spark_session import get_spark_session


SILVER_PURCHASES = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "delta_lake", "silver", "transactions")
)
SILVER_CANCELLATIONS = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "delta_lake", "silver", "cancellations")
)
GOLD_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "delta_lake", "gold", "customer_features")
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("gold-feature-eng")


def main():
    logger.info("🛠️  Gold Feature Engineering başladı")
    spark = get_spark_session("Gold-Feature-Engineering", driver_memory="3g")

    # Silver'ları oku
    df_purchases = spark.read.format("delta").load(SILVER_PURCHASES)
    df_cancellations = spark.read.format("delta").load(SILVER_CANCELLATIONS)
    logger.info(f"📊 Purchases: {df_purchases.count():,}, Cancellations: {df_cancellations.count():,}")

    # Tarih bölümlemesi
    date_range = df_purchases.agg(
        _min("invoice_date").alias("min_date"),
        _max("invoice_date").alias("max_date")
    ).collect()[0]
    min_date, max_date = date_range['min_date'], date_range['max_date']
    total_days = (max_date - min_date).days
    split_date = min_date + timedelta(days=int(total_days * 0.75))
    split_date_str = split_date.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"📅 Feature period: {min_date} → {split_date}")
    logger.info(f"📅 Target period:  {split_date} → {max_date}")

    df_feat = df_purchases.filter(col("invoice_date") < lit(split_date_str))
    df_targ = df_purchases.filter(col("invoice_date") >= lit(split_date_str))
    df_canc_feat = df_cancellations.filter(col("invoice_date") < lit(split_date_str))

    obs_date = lit(split_date_str).cast("timestamp")

    # === Features ===
    # RFM
    df_rfm = df_feat.groupBy("kullanici_ID").agg(
        datediff(obs_date, _max("invoice_date")).alias("recency_days"),
        countDistinct("ilgili_ID").alias("frequency"),
        _sum("total_price").alias("monetary")
    )

    # Avg basket
    df_basket = df_feat.groupBy("kullanici_ID", "ilgili_ID").agg(
        _sum("total_price").alias("basket_total")
    ).groupBy("kullanici_ID").agg(
        (_sum("basket_total") / countDistinct("ilgili_ID")).alias("avg_basket_value")
    )

    # Avg days between
    df_span = df_feat.groupBy("kullanici_ID").agg(
        _min("invoice_date").alias("first_purchase"),
        _max("invoice_date").alias("last_purchase"),
        countDistinct("ilgili_ID").alias("invoice_count")
    ).withColumn(
        "purchase_span_days",
        datediff(col("last_purchase"), col("first_purchase"))
    ).withColumn(
        "avg_days_between_purchases",
        when(col("invoice_count") > 1, 
             col("purchase_span_days") / (col("invoice_count") - 1))
        .otherwise(lit(0.0))
    ).select("kullanici_ID", "avg_days_between_purchases")

    # Unique products
    df_products = df_feat.groupBy("kullanici_ID").agg(
        countDistinct("stock_code").alias("unique_products")
    )

    # Cancellation count
    df_canc = df_canc_feat.groupBy("kullanici_ID").agg(
        countDistinct("ilgili_ID").alias("cancellation_count")
    )

    # Active months
    df_months = df_feat.withColumn(
        "year_month", date_format("invoice_date", "yyyy-MM")
    ).groupBy("kullanici_ID").agg(
        countDistinct("year_month").alias("active_months")
    )

    # Most active hour
    w_hour = Window.partitionBy("kullanici_ID").orderBy(col("hour_count").desc())
    df_hour = df_feat.withColumn("hour", hour("invoice_date")) \
                     .groupBy("kullanici_ID", "hour").agg(count("*").alias("hour_count")) \
                     .withColumn("rank", row_number().over(w_hour)) \
                     .filter(col("rank") == 1) \
                     .select("kullanici_ID", col("hour").alias("most_active_hour"))

    # Country
    w_country = Window.partitionBy("kullanici_ID").orderBy(col("country_count").desc())
    df_country = df_feat.groupBy("kullanici_ID", "country").agg(count("*").alias("country_count")) \
                        .withColumn("rank", row_number().over(w_country)) \
                        .filter(col("rank") == 1) \
                        .select("kullanici_ID", "country")

    # Target
    df_target = df_targ.groupBy("kullanici_ID").agg(
        _sum("total_price").alias("future_spending")
    )

    # === Birleştir ===
    df_gold = df_rfm \
        .join(df_basket, "kullanici_ID", "left") \
        .join(df_span, "kullanici_ID", "left") \
        .join(df_products, "kullanici_ID", "left") \
        .join(df_canc, "kullanici_ID", "left") \
        .join(df_months, "kullanici_ID", "left") \
        .join(df_hour, "kullanici_ID", "left") \
        .join(df_country, "kullanici_ID", "left") \
        .join(df_target, "kullanici_ID", "inner") \
        .fillna({"cancellation_count": 0})

    gold_count = df_gold.count()
    logger.info(f"📊 Gold tablo: {gold_count:,} müşteri × {len(df_gold.columns)} sütun")

    # Yaz
    df_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(GOLD_PATH)

    logger.info(f"✅ Gold yazıldı: {GOLD_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()