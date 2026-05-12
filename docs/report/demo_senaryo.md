# 🎬 Demo Senaryosu (3 dakika)

## Ön Hazırlık (Sunum Öncesi)
- [ ] Docker servisleri çalışıyor: `docker compose ps`
- [ ] Dashboard hazır: `streamlit run dashboard/app.py`
- [ ] MLflow UI açık: http://localhost:5001
- [ ] Tarayıcıda 3 tab açık:
  - Tab 1: Dashboard (localhost:8501)
  - Tab 2: MLflow (localhost:5001)
  - Tab 3: GitHub repo

## Akış

### Adım 1 — GitHub Repo (20 sn)
"Önce projenin GitHub repo'sunu göstereyim, kod yapımız böyle, README burada."
- Repo'yu aç
- Klasör yapısını göster
- Faz checklist'ini göster

### Adım 2 — Dashboard Ana Sayfa (30 sn)
"Şimdi dashboard'a geçelim. Bu projenin sonuçlarını burada interaktif olarak görebiliyoruz."
- Ana sayfaya geç
- Metric kartları oku ("100K işlem, 317 müşteri, X ülke")

### Adım 3 — EDA Sayfası (30 sn)
"Veriyi anlamak için EDA bulguları:"
- Aylık trend grafiğini göster
- "Kasım-Aralık'ta Christmas etkisi görüyoruz"
- Saatlik dağılıma in
- "Peak: 12:00-14:00 öğle arası"

### Adım 4 — Model Comparison (45 sn)
"En kritik kısım — modeller:"
- Model Comparison sayfasına geç
- En iyi modeli vurgula: "Linear Regression %66 R²"
- Grouped bar chart'ı göster
- "GBT'nin küçük datada zayıf kaldığını görüyoruz"

### Adım 5 — MLflow UI (30 sn)
"Tüm deneylerimiz MLflow'da kayıtlı:"
- MLflow tab'ına geç
- Experiment'i aç
- 5 run'ı göster
- Bir run'a tıkla → metrikleri göster

### Adım 6 — Predictions (25 sn)
"Son olarak modelin tahminleri:"
- Predictions sayfasına geç
- Feature importance'ı göster
- "Top 3: monetary, frequency, recency — RFM çekirdeği"

### Adım 7 — Kapatış (10 sn)
"Pipeline'ın tüm bileşenleri çalışıyor, kod hazır, dokümantasyon repo'da."

## Olası Sorular ve Cevaplar

S: "Neden 317 müşteri?"
C: "100K mesaj örnekledik streaming senaryosunu göstermek için. Tam dataset ile pipeline aynen çalışır, daha çok müşteri elde ederdik."

S: "Neden Linear Regression GBT'den iyi?"
C: "Küçük veride basit modeller daha iyi genelleme yapar — ensemble modeller daha çok veriye ihtiyaç duyar. Tam dataset'le GBT'nin Linear'i geçmesi beklenir."

S: "Apple Silicon'da neden host'ta Spark?"
C: "Docker amd64 emülasyonu performans sorunu yaratıyordu, native ARM64 ile 15x hız kazandık."

S: "Production'da nasıl deploy edilir?"
C: "MLflow'daki model REST API olarak deploy edilebilir, Kafka real-time event'ler verir, dashboard live olur."