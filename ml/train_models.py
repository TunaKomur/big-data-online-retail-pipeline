"""
Train ML Models with MLflow Tracking
=====================================
Gold tablosundan 5 regresyon modeli eğitir, MLflow'a kaydeder.

Çalıştırma:
    python ml/train_models.py
"""
import logging
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import mlflow
import mlflow.spark

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.regression import (
    LinearRegression, DecisionTreeRegressor, RandomForestRegressor,
    GBTRegressor, GeneralizedLinearRegression
)
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col

from src.spark_session import get_spark_session


GOLD_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "delta_lake", "gold", "customer_features")
)
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
EXPERIMENT_NAME = "customer_lifetime_value_prediction"
TARGET_COL = "future_spending"

NUMERIC_COLS = [
    "recency_days", "frequency", "monetary",
    "avg_basket_value", "avg_days_between_purchases",
    "unique_products", "cancellation_count",
    "active_months", "most_active_hour"
]
CATEGORICAL_COLS = ["country"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("ml-train")


def build_preprocessing_pipeline():
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") 
                for c in CATEGORICAL_COLS]
    encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec") 
                for c in CATEGORICAL_COLS]
    assembler = VectorAssembler(
        inputCols=NUMERIC_COLS + [f"{c}_vec" for c in CATEGORICAL_COLS],
        outputCol="features_raw",
        handleInvalid="keep"
    )
    scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                            withStd=True, withMean=False)
    return Pipeline(stages=indexers + encoders + [assembler, scaler])


def evaluate(predictions):
    metrics = {}
    for metric_name in ["rmse", "mae", "r2"]:
        ev = RegressionEvaluator(
            labelCol=TARGET_COL, predictionCol="prediction", metricName=metric_name
        )
        metrics[metric_name] = ev.evaluate(predictions)
    return metrics


def train_and_log(model, name, params, train_data, test_data):
    with mlflow.start_run(run_name=name):
        mlflow.log_params(params)
        mlflow.set_tag("model_type", name)

        start = time.time()
        fitted = model.fit(train_data)
        train_time = time.time() - start

        predictions = fitted.transform(test_data)
        metrics = evaluate(predictions)

        for k, v in metrics.items():
            mlflow.log_metric(k, v)
        mlflow.log_metric("training_time_seconds", train_time)

        try:
            mlflow.spark.log_model(fitted, "model")
        except Exception as e:
            logger.warning(f"Model log uyarısı: {e}")

        logger.info(f"   ✅ {name}: RMSE={metrics['rmse']:.2f} | "
                    f"MAE={metrics['mae']:.2f} | R²={metrics['r2']:.4f} | "
                    f"{train_time:.1f}s")

        return {"name": name, "metrics": metrics, "model": fitted}


def main():
    logger.info("🤖 ML Pipeline başladı")
    spark = get_spark_session("ML-Train", driver_memory="4g")

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    logger.info(f"📊 MLflow: {MLFLOW_URI}, Experiment: {EXPERIMENT_NAME}")

    # Veri yükle
    df = spark.read.format("delta").load(GOLD_PATH)
    logger.info(f"📊 Gold: {df.count():,} satır")

    # Outlier temizle
    upper = df.approxQuantile("future_spending", [0.99], 0.01)[0]
    df_clean = df.filter(col(TARGET_COL) <= upper).filter(col(TARGET_COL) > 0)
    logger.info(f"📊 Temizlik sonrası: {df_clean.count():,} satır")

    # Split
    train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)

    # Preprocessing
    preproc = build_preprocessing_pipeline().fit(train_df)
    train_prep = preproc.transform(train_df).cache()
    test_prep = preproc.transform(test_df).cache()
    train_prep.count()  # tetikle

    # 5 model
    models = [
        (LinearRegression(featuresCol="features", labelCol=TARGET_COL,
                          maxIter=50, regParam=0.1),
         "LinearRegression",
         {"maxIter": 50, "regParam": 0.1}),
        (DecisionTreeRegressor(featuresCol="features", labelCol=TARGET_COL,
                               maxDepth=8, minInstancesPerNode=5),
         "DecisionTree",
         {"maxDepth": 8, "minInstancesPerNode": 5}),
        (RandomForestRegressor(featuresCol="features", labelCol=TARGET_COL,
                               numTrees=50, maxDepth=10, seed=42),
         "RandomForest",
         {"numTrees": 50, "maxDepth": 10, "seed": 42}),
        (GBTRegressor(featuresCol="features", labelCol=TARGET_COL,
                      maxIter=50, maxDepth=6, stepSize=0.1, seed=42),
         "GradientBoostedTrees",
         {"maxIter": 50, "maxDepth": 6, "stepSize": 0.1, "seed": 42}),
        (GeneralizedLinearRegression(featuresCol="features", labelCol=TARGET_COL,
                                     family="gaussian", link="identity", 
                                     maxIter=50, regParam=0.1),
         "GeneralizedLinearRegression",
         {"family": "gaussian", "link": "identity", "maxIter": 50, "regParam": 0.1}),
    ]

    results = []
    for model, name, params in models:
        result = train_and_log(model, name, params, train_prep, test_prep)
        results.append(result)

    # En iyi modeli bul
    best = min(results, key=lambda r: r["metrics"]["rmse"])
    logger.info(f"\n🏆 En iyi model: {best['name']} (RMSE={best['metrics']['rmse']:.2f})")

    spark.stop()


if __name__ == "__main__":
    main()