# dashboard.py — Informe visual (API FastAPI + JWT)
import os
import requests
import pandas as pd
import streamlit as st
import plotly.express as px

# --------- Config Sidebar ---------
st.set_page_config(page_title="Informe de Contrataciones", layout="wide")
st.title("📊 Informe de Contrataciones — PoC")

with st.sidebar:
    st.header("⚙️ Configuración")
    api_url = st.text_input("API URL", value=os.getenv("API_URL", "http://localhost:8001"))
    default_user = os.getenv("API_USER", "admin")
    default_pass = os.getenv("API_PASS", "admin123")
    username = st.text_input("Usuario API", value=default_user)
    password = st.text_input("Contraseña API", value=default_pass, type="password")
    year = st.number_input("Año", min_value=2000, max_value=2100, value=2025, step=1)
    run_btn = st.button("Actualizar")

# --------- Helpers ---------
@st.cache_data(show_spinner=False)
def login_and_token(api_url: str, username: str, password: str) -> str:
    r = requests.post(
        f"{api_url}/login",
        data={"username": username, "password": password},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["access_token"]

@st.cache_data(show_spinner=False)
def fetch_json(api_url: str, token: str, path: str, params=None):
    r = requests.get(
        f"{api_url}{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params or {},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()

def melt_quarters(df: pd.DataFrame) -> pd.DataFrame:
    # df: columns [department, job, q1..q4]
    q = df.melt(id_vars=["department", "job"], value_vars=["q1","q2","q3","q4"],
                var_name="quarter", value_name="hires")
    mapper = {"q1":1, "q2":2, "q3":3, "q4":4}
    q["quarter"] = q["quarter"].map(mapper).astype(int)
    q["hires"] = pd.to_numeric(q["hires"], errors="coerce").fillna(0).astype(int)
    return q

def safe_df(obj, cols=None) -> pd.DataFrame:
    df = pd.DataFrame(obj)
    if cols:
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = df[cols]
    return df

# --------- Run ---------
if run_btn:
    try:
        token = login_and_token(api_url, username, password)
    except Exception as e:
        st.error(f"❌ Error autenticando: {e}")
        st.stop()

    # 1) Hires by quarter
    try:
        hbq_json = fetch_json(api_url, token, "/analytics/hires-by-quarter", params={"year": year})
        hbq = safe_df(hbq_json, ["department","job","q1","q2","q3","q4"])
        hbq_long = melt_quarters(hbq)
    except Exception as e:
        st.error(f"❌ Error cargando hires-by-quarter: {e}")
        st.stop()

    # 2) Departments above average
    try:
        daa_json = fetch_json(api_url, token, "/analytics/departments-above-average", params={"year": year})
        daa = safe_df(daa_json, ["id","department","hires"]).sort_values("hires", ascending=False)
    except Exception as e:
        st.error(f"❌ Error cargando departments-above-average: {e}")
        st.stop()

    # 3) Resúmenes
    hires_by_q = hbq_long.groupby("quarter")["hires"].sum().reset_index()
    hires_by_dept_q = hbq_long.groupby(["department","quarter"])["hires"].sum().reset_index()
    heat = hires_by_dept_q.pivot(index="department", columns="quarter", values="hires").fillna(0).astype(int)
    heat = heat.reindex(sorted(heat.index), axis=0)

    st.subheader(f"🗓️ Resumen {year}")
    c1, c2, c3, c4, c5 = st.columns(5)
    total = int(hires_by_q["hires"].sum())
    for i, col in enumerate([c1, c2, c3, c4], start=1):
        qval = int(hires_by_q.loc[hires_by_q["quarter"] == i, "hires"].sum())
        col.metric(f"Q{i}", qval)
    c5.metric("Total año", total)

    st.markdown("---")

    # ===== Visual 1: Barras por trimestre (stack por departamento)
    st.subheader("📦 Contrataciones por Trimestre (stack por Departamento)")
    fig1 = px.bar(
        hires_by_dept_q,
        x="quarter", y="hires", color="department",
        barmode="stack", text_auto=True,
        labels={"quarter":"Trimestre","hires":"Contrataciones","department":"Departamento"},
        category_orders={"quarter":[1,2,3,4]},
        title=f"Hires por trimestre — {year}"
    )
    st.plotly_chart(fig1, use_container_width=True)

    # ===== Visual 2: Heatmap Dept vs Trimestre
    st.subheader("🔥 Heatmap: Departamento vs Trimestre")
    fig2 = px.imshow(
        heat,
        labels=dict(x="Trimestre", y="Departamento", color="Hires"),
        x=[1,2,3,4],
        y=heat.index.tolist(),
        text_auto=True
    )
    st.plotly_chart(fig2, use_container_width=True)

    # ===== Visual 3: Departamentos sobre el promedio (barras)
    st.subheader("🏆 Departamentos sobre el promedio (año)")
    fig3 = px.bar(
        daa, x="department", y="hires",
        text_auto=True,
        labels={"department":"Departamento","hires":"Contrataciones"},
        title=f"Departamentos sobre el promedio — {year}"
    )
    fig3.update_layout(xaxis_tickangle=-30)
    st.plotly_chart(fig3, use_container_width=True)

    # ===== Datos y Descargas
    with st.expander("📥 Descargar datos"):
        c1, c2, c3 = st.columns(3)
        c1.download_button("Hires por trimestre (detalle)", hbq_long.to_csv(index=False).encode("utf-8"), file_name=f"hires_by_quarter_{year}.csv")
        c2.download_button("Departamentos sobre promedio", daa.to_csv(index=False).encode("utf-8"), file_name=f"departments_above_avg_{year}.csv")
        c3.download_button("Heatmap base (pivot)", heat.reset_index().to_csv(index=False).encode("utf-8"), file_name=f"heatmap_base_{year}.csv")

    st.success("✅ Informe generado. Tomá screenshots o imprime a PDF desde el navegador (Ctrl+P).")
else:
    st.info("Configura en la barra lateral y presiona **Actualizar** para generar el informe.")
