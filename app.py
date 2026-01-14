import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. ページ設定 ---
st.set_page_config(page_title="九州東和薬品 最強売上検索", page_icon="💊", layout="wide")

# --- カスタムCSS ---
st.markdown("""
<style>
    .main-title { font-size: 2.2em; color: #0056b3; font-weight: bold; text-align: center; border-bottom: 3px solid #0056b3; padding-bottom: 10px; }
    .stMetric { background-color: #f0f2f6; padding: 10px; border-radius: 10px; border: 1px solid #d1d5db; }
</style>
""", unsafe_allow_html=True)

# --- 2. BigQuery接続 ---
@st.cache_resource
def get_client():
    try:
        info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        return bigquery.Client.from_service_account_info(info)
    except Exception as e:
        st.error(f"認証エラー: {e}")
        return None

client = get_client()

# --- 3. タイトル ---
st.markdown('<div class="main-title">💊 九州東和薬品 最強売上検索 (SFA詳細版)</div>', unsafe_allow_html=True)

if client:
    # --- 4. データ取得クエリ ---
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python`"

    @st.cache_data(ttl=600)
    def load_data(q):
        try:
            df = client.query(q).to_dataframe()
            # ビューの実際の列名をPython側の変数名にマッピング
            rename_map = {
                '年月': '売上日',
                '品名': '商品名',
                '実績金額': '金額',
                '包装単位': '包装',
                '担当社員名': '担当者名'
            }
            df = df.rename(columns=rename_map)
            # 型変換
            df['金額'] = pd.to_numeric(df['金額'], errors='coerce').fillna(0)
            df['数量'] = pd.to_numeric(df['数量'], errors='coerce').fillna(0)
            df['売上日'] = df['売上日'].astype(str)
            return df
        except Exception as e:
            st.error(f"取得エラー: {e}")
            return pd.DataFrame()

    df = load_data(query)

    if not df.empty:
        # --- 5. サイドバー ---
        with st.sidebar:
            st.header("🔎 絞り込み条件")
            t_list = ['全て'] + sorted(df['担当者名'].unique().tolist())
            sel_t = st.selectbox("担当者名", t_list)
            
            # 担当者に紐づく得意先のみ抽出
            sub_df = df if sel_t == '全て' else df[df['担当者名'] == sel_t]
            c_list = ['全て'] + sorted(sub_df['得意先名'].unique().tolist())
            sel_c = st.selectbox("得意先名", c_list)
            
            search_kw = st.text_input("商品名検索", "")

        # フィルタリング
        f_df = sub_df.copy()
        if sel_c != '全て':
            f_df = f_df[f_df['得意先名'] == sel_c]
        if search_kw:
            f_df = f_df[f_df['商品名'].str.contains(search_kw, na=False)]

        # --- 6. サマリーメトリクス ---
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("総数量", f"{f_df['数量'].sum():,.0f}")
        m2.metric("売上金額累計", f"¥{f_df['金額'].sum():,.0f}")
        m3.metric("対象得意先", f"{f_df['得意先名'].nunique():,} 軒")
        m4.metric("取引レコード数", f"{len(f_df):,} 件")

        # --- 7. 【新機能】ビジュアル詳細分析 ---
        st.markdown("### 📈 トレンド・ランキング分析")
        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            # 月別売上推移グラフ
            st.write("▼ 月別売上金額の推移")
            monthly_sales = f_df.groupby('売上日')['金額'].sum().reset_index()
            fig_line = px.bar(monthly_sales, x='売上日', y='金額', color_discrete_sequence=['#0056b3'])
            fig_line.update_layout(height=350, margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig_line, use_container_width=True)

        with chart_col2:
            # 商品別ランキング
            st.write("▼ 売上上位商品（TOP10）")
            prod_rank = f_df.groupby('商品名')['金額'].sum().sort_values(ascending=False).head(10).reset_index()
            fig_rank = px.bar(prod_rank, x='金額', y='商品名', orientation='h', color_discrete_sequence=['#28a745'])
            fig_rank.update_layout(height=350, margin=dict(l=0, r=0, t=0, b=0), yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_rank, use_container_width=True)

        # --- 8. ピボットテーブル ---
        st.markdown("### 📊 月別・商品別詳細一覧")
        view_mode = st.radio("表示項目:", ["金額", "数量"], horizontal=True)
        val_col = '金額' if view_mode == "金額" else '数量'

        # 必要な列が揃っているか最終確認
        required = ['得意先名', '商品名', '包装', '売上日', val_col]
        if all(c in f_df.columns for c in required):
            pivot = pd.pivot_table(
                f_df, 
                index=['得意先名', '商品名', '包装'], 
                columns='売上日', 
                values=val_col, 
                aggfunc='sum', 
                fill_value=0
            )
            pivot['合計'] = pivot.sum(axis=1)
            # スタイリング
            st.dataframe(
                pivot.style.background_gradient(cmap='Greens' if view_mode=="金額" else 'Blues', axis=None).format("{:,.0f}"),
                use_container_width=True, height=500
            )
        else:
            st.error(f"列の不足: {set(required) - set(f_df.columns)}")

    else:
        st.warning("データが空です。ビューの設定を確認してください。")
