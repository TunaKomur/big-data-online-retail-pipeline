"""
Spark Session yardımcı modülü.
Tüm notebook'larda ve script'lerde aynı yapılandırmayla Spark başlatmak için kullanılır.
"""
import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "BigDataPipeline",
                       master: str = "local[2]",
                       driver_memory: str = "4g") -> SparkSession:
    """
    Standart yapılandırmayla bir Spark Session döner.

    Args:
        app_name: Spark application adı (Spark UI'da görünür)
        master: 'local[N]' (N = paralel thread sayısı)
        driver_memory: Driver belleği (örn. '2g', '4g')

    Returns:
        Yapılandırılmış SparkSession
    """
    # JAR yollarını otomatik bul
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    jar_dir = os.path.join(project_root, "jars")

    if not os.path.isdir(jar_dir):
        raise RuntimeError(
            f"JAR klasörü bulunamadı: {jar_dir}\n"
            f"Önce çalıştır: ./scripts/download_jars.sh"
        )

    jars = [os.path.join(jar_dir, j) for j in os.listdir(jar_dir) if j.endswith(".jar")]
    if not jars:
        raise RuntimeError(f"JAR klasörü boş: {jar_dir}")

    jars_str = ",".join(jars)

    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.jars", jars_str) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.driver.maxResultSize", "1g") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark