# Big Data Pipeline: Online Retail II - Customer Lifetime Value Prediction

End-to-end büyük veri projesi. Docker konteynerleri içinde Apache Kafka ile streaming veri üretimi, Apache Spark ile veri işleme, Delta Lake ile depolama, Spark MLlib ile makine öğrenmesi modelleri ve MLflow ile deney takibi.

## 📌 Proje Hakkında

Bu proje, Yıldız Teknik Üniversitesi (veya senin üniversitenin adı) Bilgisayar Mühendisliği bölümü "Büyük Veri Analizine Giriş" dersi 2025-2026 Bahar dönemi proje çalışmasıdır.

**Problem Tipi:** Regresyon  
**Hedef:** Müşteri Yaşam Boyu Değeri (Customer Lifetime Value) tahmini  
**Veri Seti:** [Online Retail II - UCI Repository](https://archive.ics.uci.edu/dataset/502/online+retail+ii)

## 🏗️ Mimari
[Excel] → [Python Producer] → [Kafka] → [Spark Streaming]
↓
[Delta Lake: Bronze → Silver → Gold]
↓
[Spark MLlib + MLflow]
↓
[Dashboard]

## 🛠️ Kullanılan Teknolojiler

- **Konteynerizasyon:** Docker, Docker Compose
- **Streaming:** Apache Kafka
- **İşleme:** Apache Spark (PySpark) Structured Streaming
- **Depolama:** Delta Lake (Bronze/Silver/Gold mimarisi)
- **ML:** Spark MLlib
- **Deney Takibi:** MLflow
- **Dashboard:** Streamlit + Plotly

## 👥 Ekip

- (İsim Soyisim)
- (İsim Soyisim)
- (İsim Soyisim)

## 🚀 Kurulum

(Yapım aşamasında — Faz 2 sonunda detaylandırılacak)

## 📁 Proje Yapısı

big-data-online-retail-pipeline/
├── docker-compose.yml
├── docker/                  # Servis Dockerfile'ları
├── producer/                # Kafka producer kodu
├── notebooks/               # Jupyter notebook'lar (EDA, Feature Eng., ML)
├── spark_jobs/              # PySpark streaming/batch işleri
├── ml/                      # ML eğitim ve değerlendirme scriptleri
├── dashboard/               # Streamlit dashboard
├── data/raw/                # Ham veri (gitignore'da)
├── delta_lake/              # Delta tabloları (gitignore'da)
├── mlruns/                  # MLflow çıktıları (gitignore'da)
└── docs/                    # Teknik rapor ve görseller

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
- [ ] Faz 1: Veri Keşfi
- [ ] Faz 2: Docker Altyapı
- [ ] Faz 3: Kafka Producer
- [ ] Faz 4: Spark Streaming + Delta Lake
- [ ] Faz 5: EDA
- [ ] Faz 6: Feature Engineering
- [ ] Faz 7: ML Modelleri + MLflow
- [ ] Faz 8: Dashboard
- [ ] Faz 9: Dokümantasyon ve Sunum

## 📝 Lisans

Eğitim amaçlıdır.