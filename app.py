import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. 基本設定 ---
st.set_page_config(page_title="Kyushu Towa SFA", layout="wide")
pd.set_option("styler.render.max_elements", 2000000)

# --- 2. ビジネスデザインCSS ---
st.markdown("""
<style>
    .main-header { background-color: #003366; padding: 1rem; color: white; text-align: center; border-radius: 8px; margin-bottom: 2rem; }
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
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python`"
    df = client.query(query).to_dataframe()
    
    # 重複列の削除
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # --- 列名の自動マッピング（Oh No 回避の要） ---
    # SQLで定義した display_xxx があれば優先、なければ既存の列を探す
    col_map = {
        'display_product_name': ['品名', '商品名'],
        'display_month': ['年月', '売上月'],
        'display_staff_name': ['担当社員名', '担当者'],
        'display_amount': ['販売金額', '実績金額']
    }
    
    for final_name, candidates in col_map.items():
        if final_name not in df.columns:
            for c in candidates:
                if c in df.columns:
                    df[final_name] = df[c]
                    break
            if final_name not in df.columns:
                df[final_name] = "N/A" # 最悪、空文字を入れてクラッシュを防ぐ

    # 2026/01を正しく並べるための処理
    df['display_month'] = df['display_month'].astype(str).str.replace('-', '/')
    
    # 型変換
    df['display_amount'] = pd.to_numeric(df['display_amount'], errors='coerce').fillna(0)
    df['数量'] = pd.to_numeric(df['数量'], errors='coerce').fillna(0)
    
    return df

# --- 4. メイン表示 ---
st.markdown('<div class="main-header"><h1>九州東和薬品 販売実績分析</h1></div>', unsafe_allow_html=True)

try:
    df = load_data()
except Exception as e:
    st.error(f"深刻なエラーが発生しました。SQLの列名を確認してください。: {e}")
    st.stop()

if not df.empty:
    with st.sidebar:
        st.header("🔎 フィルタ")
        # 担当者名寄せ（古賀さんの統一が必要な場合はここで処理）
        df['display_staff_name'] = df['display_staff_name'].replace(['優一郎', '古賀優一朗'], '古賀優一郎')
        
        t_list = ['全て'] + sorted(df['display_staff_name'].unique().tolist())
        sel_t = st.selectbox("担当者", t_list)
        
        f_df = df if sel_t == '全て' else df[df['display_staff_name'] == sel_t]
        c_list = ['全て'] + sorted(f_df['得意先名'].unique().tolist())
        sel_c = st.selectbox("得意先", c_list)
        
        kw = st.text_input("キーワード検索 (ユニークコード/商品名)")

    # フィルタリング
    display_df = f_df.copy()
    if sel_c != '全て': display_df = display_df[display_df['得意先名'] == sel_c]
    if kw: display_df = display_df[display_df['display_product_name'].str.contains(kw, na=False) | display_df['ユニークコード'].astype(str).str.contains(kw, na=False)]

    # --- 5. サマリー ---
    c1, c2, c3 = st.columns(3)
    c1.metric("販売金額 累計", f"¥{display_df['display_amount'].sum():,.0f}")
    c2.metric("販売数量 合計", f"{display_df['数量'].sum():,.0f}")
    c3.metric("対象得意先数", f"{display_df['得意先名'].nunique():,} 軒")

    # --- 6. 推移グラフ（2026/01対応） ---
    st.markdown("### 📈 月別トレンド")
    monthly = display_df.groupby('display_month')['display_amount'].sum().reset_index().sort_values('display_month')
    st.plotly_chart(px.line(monthly, x='display_month', y='display_amount', markers=True), use_container_width=True)

    # --- 7. 詳細ピボット ---
    st.markdown("### 📋 販売詳細")
    
    pivot = pd.pivot_table(
        display_df, 
        index=['得意先名', 'display_product_name', '包装単位'], 
        columns='display_month', 
        values='display_amount', 
        aggfunc='sum', 
        fill_value=0
    )
    pivot['期間合計'] = pivot.sum(axis=1)
    
    st.dataframe(
        pivot.style.background_gradient(cmap='Blues', axis=None).format("{:,.0f}"),
        use_container_width=True, height=600
    )

else:
    st.warning("データがありません。")
