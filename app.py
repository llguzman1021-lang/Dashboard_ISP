import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
from datetime import datetime
import time

# --- CONFIGURACIÓN DE CONEXIÓN (SIMPLIFICADA) ---
def conectar():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    # Cargamos directamente desde los secrets
    creds_info = dict(st.secrets["gcp_service_account"])
    # Limpiamos la llave por si acaso
    creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
    client = gspread.authorize(creds)
    return client.open("Dashboard_ISP").sheet1

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Multinet NOC Analytics", layout="wide", page_icon="💻")

# Conexión inicial
try:
    sheet = conectar()
except Exception as e:
    st.error(f"❌ Error de acceso: {e}")
    st.stop()

st.title("🛜 Multinet NOC: Dashboard en Vivo")

# --- SIDEBAR: ENTRADA DE DATOS ---
with st.sidebar:
    st.header("🌐 Entrada de Datos")
    with st.form("registro_falla", clear_on_submit=True):
        zona = st.text_input("📍 Zona")
        categoria = st.selectbox("📁 Categoría", ["Red Multinet", "Cliente Corporativo"])
        equipo = st.selectbox("🛠️ Equipo", ["OLT", "RB/Mikrotik", "Switch", "ONU", "Servidor", "Fibra Principal", "Caja NAP"])
        
        c1, c2 = st.columns(2)
        f_inicio = c1.date_input("🗓️ Fecha Inicio")
        h_inicio = c2.time_input("🕒 Hora Inicio")
        
        c3, c4 = st.columns(2)
        f_fin = c3.date_input("🗓️ Fecha Fin")
        h_fin = c4.time_input("🕒 Hora Fin")
        
        clientes = st.number_input("👥 Clientes", min_value=0, step=1)
        causa = st.selectbox("🔍 Causa", ["Corte Fibra", "Falla Eléctrica", "Hardware", "Vandalismo"])
        desc = st.text_area("📝 Descripción")
        
        btn = st.form_submit_button("Guardar Registro")
        
        if btn:
            dt_i = datetime.combine(f_inicio, h_inicio)
            dt_f = datetime.combine(f_fin, h_fin)
            duracion = round((dt_f - dt_i).total_seconds() / 3600, 2)
            
            nueva_fila = [zona, categoria, equipo, f_inicio.strftime("%d/%m/%Y"), h_inicio.strftime("%H:%M:%S"), 
                         f_fin.strftime("%d/%m/%Y"), h_fin.strftime("%H:%M:%S"), int(clientes), causa, desc, duracion]
            sheet.append_row(nueva_fila)
            st.success("✅ ¡Guardado!")
            time.sleep(1)
            st.rerun()

# --- DASHBOARD PRINCIPAL ---
try:
    records = sheet.get_all_records()
    if records:
        df = pd.DataFrame(records)
        df['Duracion_Horas'] = pd.to_numeric(df['Duracion_Horas'], errors='coerce').fillna(0)
        df['Fecha_Convertida'] = pd.to_datetime(df['Fecha_Inicio'], dayfirst=True, errors='coerce')
        
        k1, k2, k3 = st.columns(3)
        k1.metric("⏱️ MTTR", f"{df['Duracion_Horas'].mean():.2f} hrs")
        k2.metric("📉 Incidentes", len(df))
        k3.metric("👥 Impacto", f"{int(df['Clientes_Afectados'].sum())}")

        st.plotly_chart(px.area(df.groupby('Fecha_Convertida').size().reset_index(name='Cant'), 
                                x='Fecha_Convertida', y='Cant', title="Tendencia Diaria"), use_container_width=True)
        
        with st.expander("🔍 Ver Datos"):
            st.dataframe(df, use_container_width=True)
    else:
        st.info("💡 Esperando datos...")
except Exception as e:
    st.error(f"⚠️ Error visualización: {e}")
