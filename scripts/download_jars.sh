#!/bin/bash
# Spark için gerekli JAR dosyalarını indirir (Kafka + Delta Lake bağlantısı)
# Kullanım: ./scripts/download_jars.sh

set -e

JAR_DIR="$(dirname "$0")/../jars"
mkdir -p "$JAR_DIR"
cd "$JAR_DIR"

echo "📦 JAR dosyaları indiriliyor: $JAR_DIR"

# Delta Lake
curl -sLO https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar
echo "  ✓ delta-spark_2.12-3.2.0.jar"

curl -sLO https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar
echo "  ✓ delta-storage-3.2.0.jar"

# Spark-Kafka connector
curl -sLO https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar
echo "  ✓ spark-sql-kafka-0-10_2.12-3.5.0.jar"

curl -sLO https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar
echo "  ✓ spark-token-provider-kafka-0-10_2.12-3.5.0.jar"

# Kafka client
curl -sLO https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar
echo "  ✓ kafka-clients-3.5.0.jar"

# Commons pool
curl -sLO https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
echo "  ✓ commons-pool2-2.11.1.jar"

echo ""
echo "✅ Tüm JAR dosyaları indirildi: $(ls -1 | wc -l | tr -d ' ') dosya"
ls -lh