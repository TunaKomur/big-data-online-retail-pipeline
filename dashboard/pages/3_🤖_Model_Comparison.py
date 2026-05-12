"""
🤖 Model Karşılaştırma
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from utils.data_loader import load_mlflow_runs
from utils.styling import set_page_config, section_header

set_page_config("Model Karşılaştırma", "🤖")

st.title("🤖 ML Model Karşılaştırması")
st.caption("MLflow'da kaydedilen 5 regresyon modelinin performansı")

# MLflow'dan run'ları çek
with st.spinner("MLflow'dan run'lar yükleniyor..."):
    runs = load_mlflow_runs()

if runs.empty:
    st.error("❌ MLflow run'ları yüklenemedi. MLflow container çalışıyor mu?")
    st.info("Çözüm: `docker compose up -d mlflow` ve `http://localhost:5001` kontrol et")
    st.stop()

st.success(f"✅ {len(runs)} run yüklendi")

# Sadece model run'larını filtrele (feature importance run'unu hariç tut)
model_runs = runs[runs['tags.model_type'].notna()].copy()

# Metrikleri çıkar
model_runs['model_name'] = model_runs['tags.model_type']
model_runs['rmse'] = model_runs['metrics.rmse']
model_runs['mae'] = model_runs['metrics.mae']
model_runs['r2'] = model_runs['metrics.r2']
model_runs['training_time'] = model_runs['metrics.training_time_seconds']

# Sadece gerekli sütunları al ve RMSE'ye göre sırala
results = model_runs[['model_name', 'rmse', 'mae', 'r2', 'training_time']].sort_values('rmse').reset_index(drop=True)

# === En İyi Model Vurgusu ===
section_header("🏆 En İyi Model")

best = results.iloc[0]
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Model", best['model_name'])
with col2:
    st.metric("RMSE", f"£{best['rmse']:.2f}")
with col3:
    st.metric("MAE", f"£{best['mae']:.2f}")
with col4:
    st.metric("R² Score", f"{best['r2']:.4f}")

# === Karşılaştırma Tablosu ===
section_header("📊 Tüm Modeller — Detaylı Karşılaştırma")

st.dataframe(results.style.format({
    'rmse': '£{:.2f}',
    'mae': '£{:.2f}',
    'r2': '{:.4f}',
    'training_time': '{:.2f} s'
}).background_gradient(subset=['rmse', 'mae'], cmap='RdYlGn_r')
   .background_gradient(subset=['r2'], cmap='RdYlGn'),
   use_container_width=True)

# === Grouped Bar Chart (Zorunlu Görsel) ===
section_header("📈 Performans Karşılaştırma Grafiği")

col1, col2, col3 = st.columns(3)

with col1:
    fig = px.bar(results, x='model_name', y='rmse',
                 title="RMSE (Düşük = İyi)",
                 color='rmse', color_continuous_scale='RdYlGn_r',
                 labels={'model_name': 'Model', 'rmse': 'RMSE (£)'})
    fig.update_layout(height=400, xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.bar(results, x='model_name', y='mae',
                 title="MAE (Düşük = İyi)",
                 color='mae', color_continuous_scale='RdYlGn_r',
                 labels={'model_name': 'Model', 'mae': 'MAE (£)'})
    fig.update_layout(height=400, xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

with col3:
    fig = px.bar(results, x='model_name', y='r2',
                 title="R² Score (Yüksek = İyi)",
                 color='r2', color_continuous_scale='RdYlGn',
                 labels={'model_name': 'Model', 'r2': 'R²'})
    fig.add_hline(y=0, line_dash="dash", line_color="black")
    fig.update_layout(height=400, xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

# === Eğitim Süresi ===
section_header("⏱️ Eğitim Süreleri")

fig = px.bar(results, x='model_name', y='training_time',
             title="Modellerin Eğitim Süreleri",
             color='training_time', color_continuous_scale='Plasma',
             labels={'model_name': 'Model', 'training_time': 'Saniye'})
fig.update_layout(height=350, xaxis_tickangle=-45)
st.plotly_chart(fig, use_container_width=True)

# === MLflow Bilgisi ===
section_header("🔗 MLflow UI")
st.markdown("""
Tüm deneyleri detaylı olarak görmek için MLflow UI'a gidin:
- **URL:** [http://localhost:5001](http://localhost:5001)
- **Experiment:** `customer_lifetime_value_prediction`

MLflow'da görebileceğiniz şeyler:
- Her run'ın tüm parametreleri
- Her run'ın tüm metrikleri (zaman içinde değişim)
- Modellerin artifact'leri (download edilebilir)
- Run'lar arası karşılaştırma
""")