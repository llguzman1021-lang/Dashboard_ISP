import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
from datetime import datetime
import time

# --- CONFIGURACIÓN DE CONEXIÓN CON LIMPIEZA DE LLAVE ---
def conectar():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Extraer secretos
    creds_dict = st.secrets["gcp_service_account"].to_dict()
    
    # Limpiar saltos de línea en la llave
    creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
    
    # Crear credenciales
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    # Abrir el archivo
    return client.open("Dashboard_ISP").sheet1
    
# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Multinet NOC", layout="wide")

try:
    sheet = conectar()
    st.success("🚀 ¡Conexión Exitosa!") # Si ves esto, ¡ganamos!
except Exception as e:
    st.error(f"❌ Error de acceso: {e}")
    st.stop()

# --- EL RESTO DE TU CÓDIGO SIGUE IGUAL ---
st.title("🛜 Multinet NOC Dashboard")
records = sheet.get_all_records()
if records:
    df = pd.DataFrame(records)
    st.dataframe(df)
else:
    st.info("Esperando datos...")
