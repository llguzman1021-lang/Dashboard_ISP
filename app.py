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
    
    # Obtenemos los secretos
    creds_info = dict(st.secrets["gcp_service_account"])
    
    # --- LIMPIEZA EXTREMA DE LA LLAVE ---
    key = creds_info["private_key"]
    # Reemplazamos los saltos de línea literales
    key = key.replace("\\n", "\n")
    # Eliminamos espacios o caracteres invisibles al inicio y final
    key = key.strip()
    
    # REPARACIÓN DE PADDING: Si la llave está incompleta, Python falla. 
    # Esto asegura que la cadena sea válida para el decodificador.
    while len(key.split("-----")[-2].replace("\n", "")) % 4 != 0:
        key = key.replace("-----END", "=-----END")
    
    creds_info["private_key"] = key
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
    client = gspread.authorize(creds)
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
