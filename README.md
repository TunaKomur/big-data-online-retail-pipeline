# Big Data Pipeline: Online Retail II — Customer Lifetime Value Prediction

End-to-end büyük veri projesi. Apache Kafka ile streaming veri üretimi, Apache Spark ile veri işleme, Delta Lake ile depolama, Spark MLlib ile makine öğrenmesi modelleri ve MLflow ile deney takibi.

## 📌 Proje Hakkında

Bu proje Bilgisayar Mühendisliği "Büyük Veri Analizine Giriş" dersi 2025-2026 Bahar dönemi proje çalışmasıdır.

**Problem Tipi:** Regresyon  
**Hedef:** Müşteri Yaşam Boyu Değeri (Customer Lifetime Value) tahmini  
**Veri Seti:** [Online Retail II — UCI Repository](https://archive.ics.uci.edu/dataset/502/online+retail+ii)

## 🏗️ Mimari

```
[Excel Veri Seti]
       ↓
[Python Producer] (Docker) ──► [Kafka Topic] (Docker) ──► [Spark Streaming] (Host)
                                                                    ↓
                                          [Delta Lake: Bronze → Silver → Gold] (Host)
                                                                    ↓
                                                    [Spark MLlib + MLflow]
                                                                    ↓
                                                          [Streamlit Dashboard]
```

> **Not:** Apple Silicon (M1/M2/M3) üzerinde Spark, Docker amd64 emülasyonu nedeniyle kararsız çalışır. Bu projede Spark **host'ta (yerel sanal ortamda)** çalıştırılır — proje yönergesi "yerel Spark kurulumu da kabul edilir" dediği için bu yaklaşım hem kabul edilebilir hem de daha performanslıdır.

## 🛠️ Kullanılan Teknolojiler

| Katman | Teknoloji | Konum |
|--------|-----------|-------|
| Konteynerizasyon | Docker, Docker Compose | - |
| Streaming | Apache Kafka (Confluent) | Docker |
| İşleme | Apache Spark 3.5 (PySpark) | Host (sanal ortam) |
| Depolama | Delta Lake 3.2 (Bronze/Silver/Gold) | Host |
| ML | Spark MLlib | Host |
| Deney Takibi | MLflow 2.13 | Docker |
| Dashboard | Streamlit + Plotly | Host |

## 👥 Ekip

- (İsim Soyisim)
- (İsim Soyisim)
- (İsim Soyisim)

## 🚀 Kurulum

### Ön Gereksinimler
- macOS (Apple Silicon) veya Linux
- Docker Desktop (en az 8 GB RAM, 4 CPU verilmiş)
- Python 3.10
- Java 17 (Homebrew: `brew install openjdk@17`)
- Git

### Adımlar

**1. Repoyu klonla:**
```bash
git clone https://github.com/KULLANICI_ADIN/big-data-online-retail-pipeline.git
cd big-data-online-retail-pipeline
```

**2. Sanal ortam oluştur ve etkinleştir:**
```bash
python3.10 -m venv .venv
source .venv/bin/activate
```

**3. Java 17'yi sanal ortama bağla** (sanal ortam aktive dosyasına ekle):
```bash
echo 'export JAVA_HOME=/opt/homebrew/opt/openjdk@17' >> .venv/bin/activate
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> .venv/bin/activate
deactivate && source .venv/bin/activate
```

**4. Python paketlerini yükle:**
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

**5. JAR dosyalarını indir** (Spark'ın Kafka ve Delta Lake ile iletişimi için):
```bash
./scripts/download_jars.sh
```

**6. Çevre değişkenlerini ayarla:**
```bash
cp .env.example .env
```

**7. Veri setini indir:**
   - https://archive.ics.uci.edu/dataset/502/online+retail+ii adresinden ZIP indir
   - `online_retail_II.xlsx` dosyasını `data/raw/` klasörüne koy

**8. Docker servislerini başlat:**
```bash
docker compose up -d
```

İlk çalıştırmada container imajları indirilir (5-10 dk).

**9. Kafka topic'ini oluştur:**
```bash
docker exec -it kafka kafka-topics \
  --create \
  --topic online-retail-transactions \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists
```

**10. JupyterLab'i başlat (host'ta):**
```bash
jupyter lab --notebook-dir=. --no-browser
```

Terminal'de gösterilen URL'yi tarayıcıda aç.

### 🌐 Erişim Linkleri

- **Jupyter Lab:** http://localhost:8888
- **Kafka UI:** http://localhost:8080
- **MLflow:** http://localhost:5001
- **Spark UI:** http://localhost:4040 (Spark çalışırken)

### Servisleri Durdurma

```bash
# Jupyter: Ctrl+C
# Docker: aşağıdaki komut
docker compose down
```

## 🐳 Servis Mimarisi (Docker)

| Servis | Container | Port | Görev |
|--------|-----------|------|-------|
| Zookeeper | `zookeeper` | 2181 | Kafka koordinasyonu |
| Kafka | `kafka` | 9092 (iç), 9094 (host) | Mesaj kuyruğu |
| Kafka UI | `kafka-ui` | 8080 | Web yönetim arayüzü |
| Producer | `producer` | - | Kafka veri üreticisi |
| MLflow | `mlflow` | 5001 | Deney takibi |

## 📡 Kafka Producer (Faz 3)

Producer container'ı, Excel'den okuyup Kafka'ya JSON mesajlar gönderir.

## 🌊 Spark Streaming + Delta Lake (Faz 4)

Kafka'dan gelen veriyi okuyup **Bronze/Silver/Gold** Medallion mimarisiyle Delta Lake'e yazar.

### Mimari

```
Kafka → Bronze (raw)    → Silver (clean)         → Gold (Faz 6)
        ↓                 ↓
        delta/bronze/     delta/silver/
                          ├── transactions/
                          └── cancellations/
```

### Notebook'lar

| Notebook | Görev |
|----------|-------|
| `01_bronze_streaming.ipynb` | Kafka → Delta Bronze (streaming) |
| `02_silver_transformation.ipynb` | Bronze → Silver (batch) |
| `03_delta_inspection.ipynb` | Tabloları sorgulama |

### Production Script'ler

```bash
# Bronze streaming (30 saniye çalışır)
python spark_jobs/bronze_streaming.py --duration 30

# Silver batch transformation
python spark_jobs/silver_transformation.py
```

### Çalıştırma

```bash
# Test (100 mesaj, yavaş)
docker exec -e MAX_MESSAGES=100 -e PRODUCER_RATE_PER_SECOND=20 \
  -it producer python /app/kafka_producer.py

# Normal akış (10.000 mesaj)
docker exec -e MAX_MESSAGES=10000 -it producer python /app/kafka_producer.py

# Tam dataset (sınırsız)
docker exec -it producer python /app/kafka_producer.py
```

Detaylar için: `producer/README.md`

## 📁 Proje Yapısı

```
big-data-online-retail-pipeline/
├── docker-compose.yml          # Servis orkestrasyon
├── .env.example                # Çevre değişkeni şablonu
├── requirements.txt            # Python bağımlılıkları
│
├── docker/                     # Servis Dockerfile'ları
│   └── producer/
│
├── scripts/                    # Yardımcı script'ler
│   └── download_jars.sh        # JAR indirme
│
├── src/                        # Yardımcı Python modülleri
│   └── spark_session.py        # Spark session factory
│
├── jars/                       # Spark JAR'ları (gitignore)
│
├── notebooks/                  # Jupyter notebook'lar
│   └── 00_data_exploration.ipynb
│
├── producer/                   # Kafka producer kodu
├── spark_jobs/                 # PySpark streaming/batch işleri
├── ml/                         # ML eğitim/değerlendirme
├── dashboard/                  # Streamlit dashboard
│
├── data/raw/                   # Ham veri (gitignore)
├── delta_lake/                 # Delta tabloları (gitignore)
├── mlruns/                     # MLflow çıktıları (gitignore)
└── docs/                       # Teknik rapor ve görseller
```

## 📊 Veri Seti

| Özellik | Değer |
|---------|-------|
| Kaynak | UCI Machine Learning Repository |
| İşlem Sayısı | ~1.067.371 |
| Tarih Aralığı | 01/12/2009 - 09/12/2011 |
| Sütunlar | InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country |
| Boyut | 43.5 MB |

## 📈 Proje Aşamaları

- [x] Faz 0: Kurulum ve Repo
- [x] Faz 1: Veri Keşfi
- [x] Faz 2: Docker Altyapı
- [x] Faz 3: Kafka Producer
- [x] Faz 4: Spark Streaming + Delta Lake
- [ ] Faz 5: EDA
- [ ] Faz 6: Feature Engineering
- [ ] Faz 7: ML Modelleri + MLflow
- [ ] Faz 8: Dashboard
- [ ] Faz 9: Dokümantasyon ve Sunum

## 📝 Lisans

Eğitim amaçlıdır.