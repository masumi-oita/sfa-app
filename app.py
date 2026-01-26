import streamlit as st
from google.cloud import bigquery
from datetime import date
import re

PROJECT = "salesdb-479915"
DATASET = "sales_data"
client = bigquery.Client(project=PROJECT)

def norm_name(name: str) -> str:
    return re.sub(r"[ \u3000\t\r\n]+", "", (name or ""))

def get_perm(user_email: str):
    q = f"""
    SELECT can_view_admin, can_edit_admin
    FROM `{PROJECT}.{DATASET}.dim_staff_contact`
    WHERE email = @email AND is_active = TRUE
    LIMIT 1
    """
    job = client.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("email", "STRING", user_email)]
    ))
    rows = list(job.result())
    return (rows[0]["can_view_admin"], rows[0]["can_edit_admin"]) if rows else (False, False)

def upsert_staff(email: str, staff_name: str, can_view_admin: bool, can_edit_admin: bool, is_active: bool):
    q = f"""
    MERGE `{PROJECT}.{DATASET}.dim_staff_contact` T
    USING (
      SELECT
        @email AS staff_code,
        @staff_name AS staff_name,
        @email AS email,
        @is_active AS is_active,
        @can_view_admin AS can_view_admin,
        @can_edit_admin AS can_edit_admin,
        @updated_at AS updated_at
    ) S
    ON T.email = S.email
    WHEN MATCHED THEN UPDATE SET
      staff_code = S.staff_code,
      staff_name = S.staff_name,
      is_active = S.is_active,
      can_view_admin = S.can_view_admin,
      can_edit_admin = S.can_edit_admin,
      updated_at = S.updated_at
    WHEN NOT MATCHED THEN INSERT
      (staff_code, staff_name, email, is_active, can_view_admin, can_edit_admin, updated_at)
    VALUES
      (S.staff_code, S.staff_name, S.email, S.is_active, S.can_view_admin, S.can_edit_admin, S.updated_at)
    """
    client.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("email", "STRING", email),
        bigquery.ScalarQueryParameter("staff_name", "STRING", staff_name),
        bigquery.ScalarQueryParameter("is_active", "BOOL", is_active),
        bigquery.ScalarQueryParameter("can_view_admin", "BOOL", can_view_admin),
        bigquery.ScalarQueryParameter("can_edit_admin", "BOOL", can_edit_admin),
        bigquery.ScalarQueryParameter("updated_at", "DATE", date.today().isoformat()),
    ])).result()

def upsert_mapper(staff_name_src: str, login_email: str):
    staff_name_norm = norm_name(staff_name_src)
    q = f"""
    MERGE `{PROJECT}.{DATASET}.map_staff_name_to_email` T
    USING (
      SELECT
        @staff_name_src AS staff_name_src,
        @staff_name_norm AS staff_name_norm,
        @login_email AS login_email,
        'MATCHED' AS match_status,
        '岡崎真澄' AS verified_by,
        @verified_at AS verified_at,
        NULL AS note
    ) S
    ON T.staff_name_norm = S.staff_name_norm
    WHEN MATCHED THEN UPDATE SET
      staff_name_src = S.staff_name_src,
      login_email = S.login_email,
      match_status = S.match_status,
      verified_by = S.verified_by,
      verified_at = S.verified_at,
      note = S.note
    WHEN NOT MATCHED THEN INSERT
      (staff_name_src, staff_name_norm, login_email, match_status, verified_by, verified_at, note)
    VALUES
      (S.staff_name_src, S.staff_name_norm, S.login_email, S.match_status, S.verified_by, S.verified_at, S.note)
    """
    client.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("staff_name_src", "STRING", staff_name_src),
        bigquery.ScalarQueryParameter("staff_name_norm", "STRING", staff_name_norm),
        bigquery.ScalarQueryParameter("login_email", "STRING", login_email),
        bigquery.ScalarQueryParameter("verified_at", "DATE", date.today().isoformat()),
    ])).result()

st.title("管理：スタッフ登録（編集者のみ）")

user_email = st.text_input("あなたのログインemail（動作確認用）", value="okazaki@shinrai8.by-works.com")
can_view, can_edit = get_perm(user_email)

if not can_edit:
    st.warning("編集権限がありません（岡崎真澄のみ）")
    st.stop()

st.subheader("1) スタッフ追加/更新（ログインアカウント）")
with st.form("staff_form"):
    email = st.text_input("email（ログインID）")
    name = st.text_input("氏名（表示名）")
    is_active = st.checkbox("有効", value=True)
    can_view_admin = st.checkbox("管理画面閲覧可", value=False)
    can_edit_admin = st.checkbox("管理画面編集可", value=False)
    if st.form_submit_button("登録/更新"):
        upsert_staff(email=email.strip(), staff_name=name.strip(),
                     can_view_admin=can_view_admin, can_edit_admin=can_edit_admin, is_active=is_active)
        st.success("dim_staff_contact を更新しました")

st.subheader("2) 基幹担当者名 → ログインemail 紐付け（mapper）")
with st.form("mapper_form"):
    staff_name_src = st.text_input("基幹の担当者名（原文）", placeholder="木下　裕司")
    login_email = st.text_input("紐付けるemail", placeholder="kinoshita@shinrai8.by-works.com")
    if st.form_submit_button("紐付け登録/更新"):
        upsert_mapper(staff_name_src=staff_name_src, login_email=login_email)
        st.success("map_staff_name_to_email を更新しました")

st.subheader("未紐付け一覧（埋める対象）")
q = f"""
SELECT staff_name, COUNT(*) AS customer_cnt
FROM `{PROJECT}.{DATASET}.v_dim_customer_staff_current_login`
WHERE login_email IS NULL
GROUP BY staff_name
ORDER BY customer_cnt DESC
"""
st.dataframe(client.query(q).to_dataframe())
