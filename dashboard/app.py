"""
🏠 Ana Sayfa — Proje Genel Bakış
"""
import streamlit as st
from utils.data_loader import (
    load_silver_purchases, load_silver_cancellations, 
    load_gold_features, load_mlflow_runs
)
from utils.styling import set_page_config, section_header

set_page_config("Ana Sayfa", "🏠")

# Başlık
st.title("🏠 Online Retail II — Big Data Pipeline Dashboard")
st.markdown("""
Bu dashboard, **Apache Kafka + Spark + Delta Lake + MLflow** kullanarak inşa edilmiş 
uçtan uca büyük veri pipeline'ının sonuçlarını görselleştirir.
""")

st.markdown("---")

# === Proje Mimarisi ===
section_header("🏗️ Proje Mimarisi")

st.markdown("""
[Excel] → [Producer (Docker)] → [Kafka Topic] → [Spark Streaming]
↓
[Delta Lake: Bronze → Silver → Gold]
↓
[Spark MLlib + MLflow]
↓
[Bu Dashboard]
""")

# === Veri İstatistikleri ===
section_header("📊 Veri İstatistikleri")

try:
    with st.spinner("Veriler yükleniyor..."):
        purchases = load_silver_purchases()
        cancellations = load_silver_cancellations()
        gold = load_gold_features()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🛒 Toplam Alışveriş", f"{len(purchases):,}")
    
    with col2:
        st.metric("❌ İptal Sayısı", f"{len(cancellations):,}")
    
    with col3:
        st.metric("👤 ML Müşteri Sayısı", f"{len(gold):,}")
    
    with col4:
        st.metric("🌍 Ülke Sayısı", f"{purchases['country'].nunique()}")

    st.markdown("---")
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("📅 İlk İşlem", str(purchases['invoice_date'].min())[:10])
    with col2:
        st.metric("📅 Son İşlem", str(purchases['invoice_date'].max())[:10])

except Exception as e:
    st.error(f"Veri yükleme hatası: {e}")
    st.info("Delta tabloları henüz oluşturulmadıysa, Faz 4-6 script'lerini çalıştırın.")

# === Sayfaları Anlat ===
section_header("📋 Dashboard Sayfaları")

st.markdown("""
Sol menüden aşağıdaki sayfalara ulaşabilirsiniz:

- **📊 EDA** — Veri keşif analizi (zaman serisi, dağılımlar, kategorik analiz)
- **🛒 Customer Analysis** — Müşteri davranışı, top müşteriler, RFM
- **🤖 Model Comparison** — 5 modelin performans karşılaştırması, MLflow run'ları
- **🎯 Predictions** — Tahmin görselleri, feature importance, residual analizi
""")

# === Teknik Bilgi ===
with st.expander("🔧 Teknik Detaylar"):
    st.markdown("""
    **Veri Pipeline:**
    - Kafka (Confluent) — Mesaj akışı simülasyonu
    - Apache Spark 3.5 — Streaming + batch işleme
    - Delta Lake 3.2 — ACID uyumlu, versiyonlu depolama
    - Medallion architecture: Bronze (raw) → Silver (clean) → Gold (features)
    
    **ML Pipeline:**
    - Spark MLlib — 5 regresyon modeli
    - MLflow 2.13 — Deney takibi
    - Hedef: Customer Lifetime Value (CLV) tahmini
    
    **Dashboard:**
    - Streamlit — Interaktif arayüz
    - Plotly — Görselleştirme
    """)

st.markdown("---")
st.caption("🎓 Büyük Veri Analizine Giriş — Dönem Projesi 2025-2026")