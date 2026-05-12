"""
📊 EDA Sayfası — Veri Keşif Analizi
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from utils.data_loader import load_silver_purchases
from utils.styling import set_page_config, section_header

set_page_config("EDA", "📊")

st.title("📊 Keşifsel Veri Analizi")
st.caption("Silver tablosundaki temizlenmiş veriler üzerinde detaylı analiz")

# Veriyi yükle
with st.spinner("Veriler yükleniyor..."):
    df = load_silver_purchases()
    df['invoice_date'] = pd.to_datetime(df['invoice_date'])
    df['year_month'] = df['invoice_date'].dt.to_period('M').astype(str)
    df['hour'] = df['invoice_date'].dt.hour
    df['day_of_week'] = df['invoice_date'].dt.day_name()

st.success(f"✅ {len(df):,} satır yüklendi")

# === Genel İstatistikler ===
section_header("📈 Genel İstatistikler")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("İşlem Sayısı", f"{len(df):,}")
with col2:
    st.metric("Benzersiz Müşteri", f"{df['kullanici_ID'].nunique():,}")
with col3:
    st.metric("Benzersiz Ürün", f"{df['stock_code'].nunique():,}")
with col4:
    st.metric("Toplam Satış", f"£{df['total_price'].sum():,.0f}")

# === Zaman Serisi: Aylık Trend ===
section_header("📅 Aylık Satış Trendi")

monthly = df.groupby('year_month').agg(
    islem_sayisi=('total_price', 'count'),
    toplam_satis=('total_price', 'sum')
).reset_index()

fig = go.Figure()
fig.add_trace(go.Bar(
    x=monthly['year_month'], y=monthly['islem_sayisi'],
    name='İşlem Sayısı', marker_color='steelblue', yaxis='y'
))
fig.add_trace(go.Scatter(
    x=monthly['year_month'], y=monthly['toplam_satis'],
    name='Toplam Satış (£)', line=dict(color='red', width=3),
    yaxis='y2', mode='lines+markers'
))
fig.update_layout(
    title="Aylık İşlem ve Satış Trendi",
    xaxis_title="Ay",
    yaxis=dict(title='İşlem Sayısı', side='left'),
    yaxis2=dict(title='Toplam Satış (£)', overlaying='y', side='right'),
    hovermode='x unified',
    height=450
)
st.plotly_chart(fig, use_container_width=True)

# === Saatlik Dağılım ===
section_header("🕐 Saatlik Alışveriş Yoğunluğu")

hourly = df.groupby('hour').size().reset_index(name='count')
fig = px.bar(hourly, x='hour', y='count',
             title="Hangi Saatlerde Daha Çok Alışveriş Yapılıyor?",
             labels={'hour': 'Saat (24 saat)', 'count': 'İşlem Sayısı'},
             color='count', color_continuous_scale='Oranges')
fig.update_layout(height=400)
st.plotly_chart(fig, use_container_width=True)

peak_hour = hourly.loc[hourly['count'].idxmax()]
st.info(f"🎯 **En yoğun saat:** {int(peak_hour['hour'])}:00 ({int(peak_hour['count']):,} işlem)")

# === Haftanın Günleri ===
section_header("📆 Haftanın Günlerine Göre Dağılım")

day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
daily = df.groupby('day_of_week').size().reindex(day_order).reset_index(name='count')

fig = px.bar(daily, x='day_of_week', y='count',
             title="Günlere Göre İşlem Dağılımı",
             color='count', color_continuous_scale='Blues')
fig.update_layout(height=400, xaxis_title="Gün", yaxis_title="İşlem Sayısı")
st.plotly_chart(fig, use_container_width=True)

# === Ülke Dağılımı ===
section_header("🌍 Ülke Dağılımı")

country_counts = df['country'].value_counts().head(10).reset_index()
country_counts.columns = ['country', 'count']

col1, col2 = st.columns(2)

with col1:
    fig = px.bar(country_counts, x='count', y='country', orientation='h',
                 title="Top 10 Ülke (İşlem Sayısına Göre)",
                 color='count', color_continuous_scale='Teal')
    fig.update_layout(height=450, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # UK vs Diğer
    uk_count = (df['country'] == 'United Kingdom').sum()
    other_count = len(df) - uk_count
    
    fig = px.pie(values=[uk_count, other_count],
                 names=['United Kingdom', 'Diğer Ülkeler'],
                 title="UK vs Diğer Ülkeler",
                 color_discrete_sequence=['#3498db', '#e67e22'],
                 hole=0.4)
    fig.update_layout(height=450)
    st.plotly_chart(fig, use_container_width=True)

# === Fiyat ve Quantity Dağılımı ===
section_header("💰 Fiyat ve Miktar Dağılımı")

col1, col2 = st.columns(2)

with col1:
    fig = px.histogram(df[df['unit_price'] < df['unit_price'].quantile(0.99)],
                       x='unit_price', nbins=50,
                       title="Birim Fiyat Dağılımı",
                       labels={'unit_price': 'Birim Fiyat (£)'},
                       color_discrete_sequence=['#9b59b6'])
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.histogram(df[df['quantity'] < df['quantity'].quantile(0.99)],
                       x='quantity', nbins=50,
                       title="Miktar Dağılımı",
                       labels={'quantity': 'Miktar'},
                       color_discrete_sequence=['#16a085'])
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

# === Top Ürünler ===
section_header("🏆 Top 15 Ürün")

top_products = df.groupby(['stock_code', 'description']).agg(
    total_sales=('total_price', 'sum'),
    quantity_sold=('quantity', 'sum')
).reset_index().nlargest(15, 'total_sales')
top_products['description_short'] = top_products['description'].str[:40]

fig = px.bar(top_products, x='total_sales', y='description_short',
             orientation='h',
             title="En Çok Gelir Getiren Ürünler",
             color='total_sales', color_continuous_scale='Greens',
             labels={'total_sales': 'Toplam Satış (£)', 'description_short': 'Ürün'})
fig.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
st.plotly_chart(fig, use_container_width=True)