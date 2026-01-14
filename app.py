import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. 基本設定 ---
st.set_page_config(page_title="Kyushu Towa SFA Dashboard", layout="wide")
pd.set_option("styler.render.max_elements", 2000000)

# --- 2. ビジネスデザインCSS ---
st.markdown("""
<style>
    .main-header { background-color: #003366; padding: 1.5rem; color: white; text-align: center; border-radius: 8px; margin-bottom: 2rem; }
    .stMetric { background-color: white; border: 1px solid #ddd; padding: 15px; border-radius: 10px; }
</style>
""", unsafe_allow_html=True)

# --- 3. 認証・データ取得 ---
@st.cache_resource
def get_client():
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    return bigquery.Client.from_service_account_info(info)

@st.cache_data(ttl=600)
def load_data():
    client = get_client()
    # SQLで「売上月」でソートして取得
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python` ORDER BY `売上月` ASC"
    df = client.query(query).to_dataframe()
    
    # --- 重要：重複列を強制的に1つに絞る（Oh No! エラーの直接的な対策） ---
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # 日付表示の整形
    df['表示月'] = df['売上月'].astype(str).str.replace('-', '/')
    
    # 数値変換の徹底
    df['販売金額'] = pd.to_numeric(df['販売金額'], errors='
