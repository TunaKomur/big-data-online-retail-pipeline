"""
🛒 Müşteri Analizi
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from utils.data_loader import load_gold_features
from utils.styling import set_page_config, section_header

set_page_config("Müşteri Analizi", "🛒")

st.title("🛒 Müşteri Davranışı Analizi")
st.caption("Gold tablosu — müşteri seviyesinde feature'lar ve segmentler")

# Veriyi yükle
with st.spinner("Müşteri verileri yükleniyor..."):
    df = load_gold_features()

st.success(f"✅ {len(df):,} müşteri yüklendi")

# === RFM Metrikleri ===
section_header("📊 RFM Metrikleri (Recency, Frequency, Monetary)")

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Median Recency", f"{df['recency_days'].median():.0f} gün")
with col2:
    st.metric("Median Frequency", f"{df['frequency'].median():.0f} fatura")
with col3:
    st.metric("Median Monetary", f"£{df['monetary'].median():.2f}")

# RFM dağılımları
col1, col2, col3 = st.columns(3)

with col1:
    fig = px.histogram(df, x='recency_days', nbins=30,
                       title="Recency Dağılımı",
                       color_discrete_sequence=['#e74c3c'])
    fig.update_layout(height=350, xaxis_title="Recency (gün)")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.histogram(df, x='frequency', nbins=30,
                       title="Frequency Dağılımı",
                       color_discrete_sequence=['#3498db'])
    fig.update_layout(height=350, xaxis_title="Frequency (fatura)")
    st.plotly_chart(fig, use_container_width=True)

with col3:
    fig = px.histogram(df, x='monetary', nbins=30,
                       title="Monetary Dağılımı",
                       color_discrete_sequence=['#27ae60'])
    fig.update_layout(height=350, xaxis_title="Monetary (£)", yaxis_type='log')
    st.plotly_chart(fig, use_container_width=True)

# === Top Müşteriler ===
section_header("🏆 Top 10 Müşteri")

top_customers = df.nlargest(10, 'monetary')[
    ['kullanici_ID', 'monetary', 'frequency', 'recency_days', 'country']
].reset_index(drop=True)

fig = px.bar(top_customers, x='monetary', y=top_customers['kullanici_ID'].astype(str),
             orientation='h',
             title="En Çok Harcayan Müşteriler",
             labels={'monetary': 'Toplam Harcama (£)', 'y': 'Müşteri ID'},
             color='monetary', color_continuous_scale='YlOrBr')
fig.update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
st.plotly_chart(fig, use_container_width=True)

st.dataframe(top_customers.style.format({
    'monetary': '£{:,.2f}',
    'frequency': '{:.0f}',
    'recency_days': '{:.0f}',
    'kullanici_ID': '{:.0f}'
}), use_container_width=True)

# === Müşteri Aktivite Heatmap ===
section_header("🕐 Müşteri Aktivite Paterni")

col1, col2 = st.columns(2)

with col1:
    fig = px.histogram(df, x='most_active_hour', nbins=24,
                       title="En Çok Hangi Saatte Alışveriş Yapıyorlar?",
                       labels={'most_active_hour': 'Saat'},
                       color_discrete_sequence=['#e67e22'])
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.histogram(df, x='active_months', nbins=20,
                       title="Kaç Farklı Ayda Alışveriş Yapıyorlar?",
                       labels={'active_months': 'Aktif Ay Sayısı'},
                       color_discrete_sequence=['#9b59b6'])
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

# === Ülke Bazlı Dağılım ===
section_header("🌍 Ülkelere Göre Müşteri Dağılımı")

country_dist = df['country'].value_counts().head(10).reset_index()
country_dist.columns = ['country', 'count']

fig = px.bar(country_dist, x='count', y='country', orientation='h',
             title="Top 10 Ülke — Müşteri Sayısı",
             color='count', color_continuous_scale='Blues')
fig.update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
st.plotly_chart(fig, use_container_width=True)

# === Müşteri Segmentasyonu ===
section_header("👥 Müşteri Segmentasyonu (RFM)")

# Basit segmentasyon: Recency düşük + Monetary yüksek = "Şampiyon"
df['recency_score'] = pd.qcut(df['recency_days'], 3, labels=['Yüksek', 'Orta', 'Düşük'], duplicates='drop')
df['monetary_score'] = pd.qcut(df['monetary'], 3, labels=['Düşük', 'Orta', 'Yüksek'], duplicates='drop')

segment_counts = df.groupby(['recency_score', 'monetary_score']).size().reset_index(name='count')

fig = px.density_heatmap(segment_counts, x='recency_score', y='monetary_score', z='count',
                         title="Müşteri Segmentleri (Recency × Monetary)",
                         color_continuous_scale='Viridis',
                         labels={'recency_score': 'Recency (Yüksek = Yakın zamanda)',
                                 'monetary_score': 'Monetary'})
fig.update_layout(height=400)
st.plotly_chart(fig, use_container_width=True)