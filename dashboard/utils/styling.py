"""
Dashboard için ortak stil yardımcıları.
"""
import streamlit as st


def set_page_config(title: str, icon: str = "📊"):
    """Tüm sayfalarda kullanılacak ortak sayfa ayarları."""
    st.set_page_config(
        page_title=f"{title} | Online Retail Pipeline",
        page_icon=icon,
        layout="wide",
        initial_sidebar_state="expanded"
    )


def show_metric_card(label: str, value: str, delta: str = None):
    """Tek bir metric göster."""
    st.metric(label=label, value=value, delta=delta)


def section_header(title: str, description: str = None):
    """Bölüm başlığı."""
    st.markdown(f"## {title}")
    if description:
        st.caption(description)
    st.markdown("---")