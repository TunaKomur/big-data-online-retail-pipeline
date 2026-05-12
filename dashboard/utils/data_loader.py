"""
Dashboard için veri yükleme yardımcıları.
Delta Lake tablolarını ve MLflow run'larını cache'li olarak okur.
"""
import os
import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession


# Proje root path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


@st.cache_resource
def get_spark():
    """Cache'li Spark Session — tüm sayfalar aynı session'ı kullanır."""
    import sys
    sys.path.insert(0, PROJECT_ROOT)
    from src.spark_session import get_spark_session
    
    return get_spark_session(
        app_name="Dashboard",
        master="local[2]",
        driver_memory="2g"
    )


@st.cache_data(ttl=600)  # 10 dakika cache
def load_silver_purchases():
    """Silver purchases tablosunu pandas DataFrame olarak yükle."""
    spark = get_spark()
    path = os.path.join(PROJECT_ROOT, "delta_lake", "silver", "transactions")
    df = spark.read.format("delta").load(path)
    return df.toPandas()


@st.cache_data(ttl=600)
def load_silver_cancellations():
    """Silver cancellations tablosunu pandas DataFrame olarak yükle."""
    spark = get_spark()
    path = os.path.join(PROJECT_ROOT, "delta_lake", "silver", "cancellations")
    df = spark.read.format("delta").load(path)
    return df.toPandas()


@st.cache_data(ttl=600)
def load_gold_features():
    """Gold customer features tablosunu pandas DataFrame olarak yükle."""
    spark = get_spark()
    path = os.path.join(PROJECT_ROOT, "delta_lake", "gold", "customer_features")
    df = spark.read.format("delta").load(path)
    return df.toPandas()


@st.cache_data(ttl=600)
def load_mlflow_runs():
    """MLflow'dan tüm run'ları çek."""
    import mlflow
    mlflow.set_tracking_uri("http://localhost:5001")
    
    try:
        experiment = mlflow.get_experiment_by_name("customer_lifetime_value_prediction")
        if experiment is None:
            return pd.DataFrame()
        
        runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
        return runs
    except Exception as e:
        st.error(f"MLflow bağlantı hatası: {e}")
        return pd.DataFrame()