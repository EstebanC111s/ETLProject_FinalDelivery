from __future__ import annotations

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from util_db import get_engine

# ==========================
# Cargar variables de entorno (.env)
# ==========================
load_dotenv()

# ==========================
# Config de página
# ==========================
st.set_page_config(
    page_title="RUPS – Real-Time Dashboard",
    layout="wide",
)

st.title("📊 RUPS – Real-Time Dashboard (Kafka + Modelo Dimensional)")
st.caption(
    "Datos que vienen del modelo dimensional (dim_geo, dim_prestadores, "
    "dim_prestacion_geo, dim_calidad_geo) y llegan vía Kafka a la tabla "
    "`rt_rups_metrics`."
)

# Conectar a la BD usando util_db (elige DB_URL_HOST cuando corres en el host)
engine = get_engine()

# ==========================
# Carga de datos
# ==========================
@st.cache_data(ttl=5)
def load_data() -> pd.DataFrame:
    query = """
        SELECT
          ts_ingestion,
          departamento,
          municipio,
          provider_id,
          nombre,
          servicio,
          estado,
          clasificacion,
          es_publica,
          total_prestadores,
          acueducto_total,
          alcantarillado_total,
          aseo_total,
          operativos_total,
          suspendidos_total
        FROM rt_rups_metrics
        ORDER BY ts_ingestion DESC
        LIMIT 1000;
    """
    return pd.read_sql(query, engine)


df = load_data()

if df.empty:
    st.warning("No hay datos en `rt_rups_metrics`. Ejecuta el producer primero.")
    st.stop()

if st.button("🔄 Refrescar datos"):
    load_data.clear()
    df = load_data()

# ==========================
# Filtros globales
# ==========================
c1, c2, c3 = st.columns(3)

with c1:
    deptos = ["(Todos)"] + sorted(df["departamento"].dropna().unique().tolist())
    sel_depto = st.selectbox("Departamento", deptos)

with c2:
    munis = ["(Todos)"] + sorted(df["municipio"].dropna().unique().tolist())
    sel_muni = st.selectbox("Municipio", munis)

with c3:
    servicios = ["(Todos)"] + sorted(df["servicio"].dropna().unique().tolist())
    sel_serv = st.selectbox("Servicio", servicios)

f = df.copy()
if sel_depto != "(Todos)":
    f = f[f["departamento"] == sel_depto]
if sel_muni != "(Todos)":
    f = f[f["municipio"] == sel_muni]
if sel_serv != "(Todos)":
    f = f[f["servicio"] == sel_serv]

if f.empty:
    st.info("No hay datos para esos filtros.")
    st.stop()

# =====================================================
# OBJETIVO 1: Concentración de prestadores por municipio
# =====================================================
st.subheader("🎯 Objetivo 1 – Concentración de prestadores por municipio")

k_prestadores = int(f["provider_id"].nunique())
k_registros = len(f)

estados = f["estado"].fillna("SIN ESTADO").value_counts()
top_estado = estados.index[0]
top_estado_val = int(estados.iloc[0])

k1, k2, k3 = st.columns(3)
k1.metric("Prestadores únicos (filtro actual)", k_prestadores)
k2.metric("Registros recientes (mensajes)", k_registros)
k3.metric("Estado dominante", f"{top_estado} ({top_estado_val})")

st.markdown("**Distribución de prestadores por municipio (más a menos):**")

g_muni = (
    f.groupby(["departamento", "municipio"])["provider_id"]
    .nunique()
    .reset_index(name="num_prestadores")
    .sort_values("num_prestadores", ascending=False)
)

if not g_muni.empty:
    chart_muni = g_muni.set_index("municipio")["num_prestadores"]
    st.bar_chart(chart_muni)
    st.caption("✅ Objetivo 1: identificamos los municipios con mayor/menor concentración de prestadores.")
else:
    st.info("No hay municipios para graficar.")

# =====================================================
# OBJETIVO 2: Estado operativo por tipo de servicio
# =====================================================
st.subheader("🎯 Objetivo 2 – Estado operativo por tipo de servicio")

g_serv_estado = (
    f.groupby(["servicio", "estado"])["provider_id"]
    .nunique()
    .reset_index(name="num_prestadores")
)

if not g_serv_estado.empty:
    pivot_se = g_serv_estado.pivot(
        index="servicio", columns="estado", values="num_prestadores"
    ).fillna(0)
    st.bar_chart(pivot_se)
    st.caption(
        "✅ Objetivo 2: comparamos cuántos prestadores hay en cada estado por tipo de servicio."
    )
else:
    st.info("No hay datos suficientes para servicio vs estado.")

# =====================================================
# OBJETIVO 3: Tipo de oferta de servicio (combinaciones)
# =====================================================
st.subheader("🎯 Objetivo 3 – Tipo de oferta de servicio por prestador")

def clasificar_oferta(row) -> str:
    a = (row["acueducto_total"] or 0) > 0
    b = (row["alcantarillado_total"] or 0) > 0
    c = (row["aseo_total"] or 0) > 0

    if not (a or b or c):
        return "Sin información"
    if a and b and c:
        return "AAA (Acueducto+Alcantarillado+Aseo)"
    if a and b:
        return "Acueducto + Alcantarillado"
    if a and c:
        return "Acueducto + Aseo"
    if b and c:
        return "Alcantarillado + Aseo"
    if a:
        return "Solo Acueducto"
    if b:
        return "Solo Alcantarillado"
    if c:
        return "Solo Aseo"
    return "Otro"

f_tipo = f.copy()
f_tipo["tipo_oferta"] = f_tipo.apply(clasificar_oferta, axis=1)

g_tipo = (
    f_tipo.groupby("tipo_oferta")["provider_id"]
    .nunique()
    .reset_index(name="num_prestadores")
    .sort_values("num_prestadores", ascending=False)
)

if not g_tipo.empty:
    chart_tipo = g_tipo.set_index("tipo_oferta")["num_prestadores"]
    st.bar_chart(chart_tipo)
    st.caption(
        "✅ Objetivo 3: caracterizamos a los prestadores según la combinación de servicios que ofrecen."
    )
else:
    st.info("No hay datos para clasificar el tipo de oferta.")

# =====================================================
# OBJETIVO 4: Prestadores críticos por suspensiones
# =====================================================
st.subheader("🎯 Objetivo 4 – Prestadores críticos según suspensiones")

f_crit = f.copy()
f_crit["servicios_totales_monitoreados"] = (
    f_crit["operativos_total"].fillna(0) + f_crit["suspendidos_total"].fillna(0)
)

# evitar división por cero
mask = f_crit["servicios_totales_monitoreados"] > 0
f_crit = f_crit[mask].copy()

f_crit["tasa_suspension"] = f_crit["suspendidos_total"] / f_crit[
    "servicios_totales_monitoreados"
]

ranking = (
    f_crit.sort_values(["tasa_suspension", "suspendidos_total"], ascending=False)
    .loc[:, [
        "departamento",
        "municipio",
        "provider_id",
        "nombre",
        "servicio",
        "operativos_total",
        "suspendidos_total",
        "tasa_suspension",
    ]]
    .head(15)
)

if not ranking.empty:
    st.dataframe(ranking)
    st.caption(
        "✅ Objetivo 4: identificamos prestadores con mayor proporción de servicios suspendidos "
        "para priorizar seguimiento."
    )
else:
    st.info("No se encontraron prestadores con información de suspensiones.")
