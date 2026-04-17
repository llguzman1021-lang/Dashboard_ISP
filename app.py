import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os

st.set_page_config(page_title="Multinet NOC Analytics | Enterprise Operations", layout="wide", page_icon="🌐")

# Conexión a Neon
@st.cache_resource
def get_engine():
    return create_engine(st.secrets["neon_dsn"])

engine = get_engine()

# Cargar datos desde Neon
@st.cache_data(ttl=300)
def load_data():
    query = "SELECT * FROM incidents WHERE deleted = FALSE"
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df

df = load_data()

# ---------------------------
# UI DEL DASHBOARD (NO CAMBIADA)
# ---------------------------

st.title("📊 Dashboard de Incidencias - Multinet NOC Analytics")

st.subheader("Filtros")
col1, col2 = st.columns(2)

zona_filter = col1.selectbox("Zona", ["Todas"] + sorted(df["zona"].dropna().unique().tolist()))
servicio_filter = col2.selectbox("Servicio", ["Todos"] + sorted(df["servicio"].dropna().unique().tolist()))

filtered_df = df.copy()

if zona_filter != "Todas":
    filtered_df = filtered_df[filtered_df["zona"] == zona_filter]

if servicio_filter != "Todos":
    filtered_df = filtered_df[filtered_df["servicio"] == servicio_filter]

st.dataframe(filtered_df)

# ---------------------------
# FORMULARIO PARA NUEVAS INCIDENCIAS
# ---------------------------

st.subheader("➕ Registrar nueva incidencia")

with st.form("form_incidente"):
    zona = st.text_input("Zona")
    servicio = st.text_input("Servicio")
    categoria = st.text_input("Categoria")
    equipo = st.text_input("Equipo Afectado")
    fecha_inicio = st.date_input("Fecha Inicio")
    hora_inicio = st.time_input("Hora Inicio")
    fecha_fin = st.date_input("Fecha Fin")
    hora_fin = st.time_input("Hora Fin")
    clientes = st.number_input("Clientes Afectados", min_value=0)
    causa = st.text_input("Causa Raiz")
    descripcion = st.text_area("Descripción")
    duracion = st.number_input("Duración (horas)", min_value=0.0)
    conocimiento = st.text_input("Conocimiento de Tiempos")

    submitted = st.form_submit_button("Guardar")

if submitted:
    insert_sql = text("""
        INSERT INTO incidents (
            worksheet_name, gsheet_id, zona, servicio, categoria, equipo_afectado,
            fecha_inicio, hora_inicio, fecha_fin, hora_fin,
            clientes_afectados, causa_raiz, descripcion,
            duracion_horas, conocimiento_tiempos
        )
        VALUES ('APP', 0, :zona, :servicio, :categoria, :equipo,
                :fecha_inicio, :hora_inicio, :fecha_fin, :hora_fin,
                :clientes, :causa, :descripcion,
                :duracion, :conocimiento)
    """)

    with engine.begin() as conn:
        conn.execute(insert_sql, {
            "zona": zona,
            "servicio": servicio,
            "categoria": categoria,
            "equipo": equipo,
            "fecha_inicio": fecha_inicio,
            "hora_inicio": hora_inicio,
            "fecha_fin": fecha_fin,
            "hora_fin": hora_fin,
            "clientes": clientes,
            "causa": causa,
            "descripcion": descripcion,
            "duracion": duracion,
            "conocimiento": conocimiento
        })

    st.success("Incidencia guardada correctamente.")
    st.cache_data.clear()
