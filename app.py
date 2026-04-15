import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
from datetime import datetime
import time

# --- FUNCIÓN DE CONEXIÓN CORREGIDA ---
def conectar():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # 1. Intentamos leer los Secrets de Streamlit
    try:
        # Convertimos los secretos a un diccionario de Python
        creds_info = st.secrets["gcp_service_account"]
        # Usamos from_json_keyfile_dict para NO buscar un archivo .json
        creds = ServiceAccountCredentials.from_json_keyfile_dict(dict(creds_info), scope)
        client = gspread.authorize(creds)
        return client.open("Dashboard_ISP").sheet1
    except Exception as e:
        st.error(f"❌ Error al leer los Secrets: {e}")
        st.stop()

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Multinet NOC Analytics", layout="wide", page_icon="💻")

# Intentar conectar al inicio
sheet = conectar()

st.title("🛜 Multinet NOC: Gestión de Incidentes")

# --- DASHBOARD Y FORMULARIO ---
# (Aquí va el resto de tu código de gráficas y sidebar que ya tenías)
# Solo asegúrate de usar la variable 'sheet' que definimos arriba.

try:
    records = sheet.get_all_records()
    if records:
        df = pd.DataFrame(records)
        st.write("### Datos actuales")
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Hoja de cálculo vacía.")
except Exception as e:
    st.error(f"Error al cargar datos: {e}")
