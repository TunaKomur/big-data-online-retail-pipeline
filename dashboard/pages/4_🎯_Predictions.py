"""
🎯 Tahmin Görselleri
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from utils.data_loader import load_gold_features, load_mlflow_runs, get_spark
from utils.styling import set_page_config, section_header

set_page_config("Tahminler", "🎯")

st.title("🎯 Tahmin ve Feature Analizi")
st.caption("En iyi modelin tahminleri, residual analizi ve feature importance")

# === Feature Importance ===
section_header("📊 Feature Importance")

st.markdown("""
Random Forest ve Gradient Boosted Trees modellerinden çıkarılan feature importance değerleri. 
Modelin tahmin yaparken hangi feature'lara daha fazla önem verdiğini gösterir.
""")

# MLflow'dan feature importance metriklerini çek
runs = load_mlflow_runs()

if not runs.empty:
    # Feature importance run'ı bul (rf_importance_ ile başlayan metrikler)
    rf_imp_cols = [c for c in runs.columns if c.startswith('metrics.rf_importance_')]
    gbt_imp_cols = [c for c in runs.columns if c.startswith('metrics.gbt_importance_')]
    
    if rf_imp_cols and gbt_imp_cols:
        # Son run'dan al
        latest_run = runs.iloc[0]
        
        rf_data = []
        for col in rf_imp_cols:
            feat_name = col.replace('metrics.rf_importance_', '')
            val = latest_run.get(col)
            if pd.notna(val):
                rf_data.append({'feature': feat_name, 'importance': val})
        
        gbt_data = []
        for col in gbt_imp_cols:
            feat_name = col.replace('metrics.gbt_importance_', '')
            val = latest_run.get(col)
            if pd.notna(val):
                gbt_data.append({'feature': feat_name, 'importance': val})
        
        rf_df = pd.DataFrame(rf_data).sort_values('importance', ascending=True)
        gbt_df = pd.DataFrame(gbt_data).sort_values('importance', ascending=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(rf_df, x='importance', y='feature', orientation='h',
                         title="Random Forest — Feature Importance",
                         color='importance', color_continuous_scale='Greens')
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(gbt_df, x='importance', y='feature', orientation='h',
                         title="Gradient Boosted Trees — Feature Importance",
                         color='importance', color_continuous_scale='Blues')
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        
        # Top 3 feature
        st.markdown("### 🏆 En Önemli Feature'lar")
        top_rf = rf_df.sort_values('importance', ascending=False).head(3)
        st.markdown("**Random Forest'a göre:**")
        for i, row in top_rf.iterrows():
            st.markdown(f"- `{row['feature']}` — {row['importance']:.4f}")
    else:
        st.warning("Feature importance verisi MLflow'da bulunamadı.")
else:
    st.error("MLflow'dan veri alınamadı.")

# === Tahmin Analizi (Gerçek vs Tahmin) ===
section_header("🎯 En İyi Modelin Tahmin Analizi")

st.markdown("""
En iyi modelin test setindeki tahminlerini görelim. Bu analiz model çıktısını yeniden 
hesaplayarak yapıyor (cache'leme için).
""")

if st.button("🚀 Tahminleri Çalıştır"):
    with st.spinner("Model çalıştırılıyor (1-2 dakika)..."):
        try:
            import sys
            import os
            sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
            
            from pyspark.ml import Pipeline
            from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
            from pyspark.ml.regression import LinearRegression
            from pyspark.sql.functions import col
            
            spark = get_spark()
            
            GOLD_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "delta_lake", "gold", "customer_features"))
            df_spark = spark.read.format("delta").load(GOLD_PATH)
            
            # Outlier temizliği
            upper = df_spark.approxQuantile("future_spending", [0.99], 0.01)[0]
            df_clean = df_spark.filter(col("future_spending") <= upper).filter(col("future_spending") > 0)
            
            # Pipeline
            indexers = [StringIndexer(inputCol="country", outputCol="country_idx", handleInvalid="keep")]
            encoders = [OneHotEncoder(inputCol="country_idx", outputCol="country_vec")]
            numeric_cols = ["recency_days", "frequency", "monetary", "avg_basket_value",
                            "avg_days_between_purchases", "unique_products", "cancellation_count",
                            "active_months", "most_active_hour"]
            assembler = VectorAssembler(inputCols=numeric_cols + ["country_vec"],
                                        outputCol="features_raw", handleInvalid="keep")
            scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                                    withStd=True, withMean=False)
            pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler])
            
            # Split
            train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)
            
            # Fit + transform
            prep_model = pipeline.fit(train_df)
            train_prep = prep_model.transform(train_df)
            test_prep = prep_model.transform(test_df)
            
            # Train Linear Regression (en iyi model)
            lr = LinearRegression(featuresCol="features", labelCol="future_spending",
                                  maxIter=50, regParam=0.1)
            model = lr.fit(train_prep)
            predictions = model.transform(test_prep)
            
            # Pandas'a çevir
            pred_pd = predictions.select("future_spending", "prediction").toPandas()
            pred_pd['residual'] = pred_pd['future_spending'] - pred_pd['prediction']
            
            # Cache'e koy
            st.session_state['predictions'] = pred_pd
            st.success("✅ Tahminler hazır!")
        except Exception as e:
            st.error(f"Hata: {e}")

if 'predictions' in st.session_state:
    pred_pd = st.session_state['predictions']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Scatter: Gerçek vs Tahmin
        max_val = max(pred_pd['future_spending'].max(), pred_pd['prediction'].max())
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=pred_pd['future_spending'], y=pred_pd['prediction'],
            mode='markers', name='Tahmin',
            marker=dict(color='steelblue', size=8, opacity=0.6)
        ))
        fig.add_trace(go.Scatter(
            x=[0, max_val], y=[0, max_val],
            mode='lines', name='Mükemmel Tahmin',
            line=dict(color='red', dash='dash', width=2)
        ))
        fig.update_layout(
            title="Gerçek vs Tahmin (Linear Regression)",
            xaxis_title="Gerçek Harcama (£)",
            yaxis_title="Tahmin Edilen Harcama (£)",
            height=450
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Residual plot
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=pred_pd['prediction'], y=pred_pd['residual'],
            mode='markers', name='Residual',
            marker=dict(color='coral', size=8, opacity=0.6)
        ))
        fig.add_hline(y=0, line_dash="dash", line_color="red", line_width=2)
        fig.update_layout(
            title="Residual Analizi",
            xaxis_title="Tahmin Edilen (£)",
            yaxis_title="Residual (Hata)",
            height=450
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Residual istatistikleri
    st.markdown("### 📊 Residual İstatistikleri")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Mean Residual", f"£{pred_pd['residual'].mean():.2f}")
    with col2:
        st.metric("Std Residual", f"£{pred_pd['residual'].std():.2f}")
    with col3:
        st.metric("Max Hata", f"£{pred_pd['residual'].abs().max():.2f}")
    with col4:
        st.metric("Min Hata", f"£{pred_pd['residual'].abs().min():.2f}")
    
    st.info("""
    💡 **Yorum:** 
    - Mean residual 0'a yakın olmalı → modelin sistematik yanlılığı yok
    - Residual'lar 0 etrafında rastgele dağılmalı → modelin yapamadığı bir patern yok
    """)
else:
    st.info("👆 Yukarıdaki butona tıklayarak tahminleri başlatın")