import streamlit as st

st.write("secrets keys:", list(st.secrets.keys()))
st.write("has gcp_service_account:", "gcp_service_account" in st.secrets)
st.write("gcp keys:", list(st.secrets["gcp_service_account"].keys()))
