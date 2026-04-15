import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
from datetime import datetime
import time
import base64

# --- CONEXIÓN CON AUTO-REPARACIÓN DE LLAVE ---
def conectar():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Extraemos los secretos
    creds_info = st.secrets["gcp_service_account"]
    # Convertimos a diccionario real
    creds_dict = {key: val for key, val in creds_info.items()}
    
    # --- MOTOR DE LIMPIEZA EXTREMA ---
    raw_key = creds_dict["private_key"]
    
    # 1. Corregir saltos de línea
    clean_key = raw_key.replace("\\n", "\n")
    
    # 2. Reparar el Padding de Base64 (Solución al error rojo)
    # Extraemos solo el cuerpo de la llave entre los marcadores -----
    parts = clean_key.split("-----")
    if len(parts) >= 3:
        header = "-----BEGIN PRIVATE KEY-----"
        footer = "-----END PRIVATE KEY-----"
        # Limpiamos el contenido interno de cualquier espacio o salto
        content = parts[2].strip().replace("\n", "").replace(" ", "")
        
        # Agregamos los "=" faltantes que causan el error de padding
        while len(content) % 4 != 0:
            content += "="
            
        # Reconstruimos la llave perfecta
        clean_key = f"{header}\n{content}\n{footer}\n"

    creds_dict["private_key"] = clean_key
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client.open("Dashboard_ISP").sheet1

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Multinet NOC", layout="wide")

try:
    sheet = conectar()
    st.success("✅ ¡Conexión establecida con éxito!")
except Exception as e:
    st.error(f"❌ Error de acceso: {e}")
    st.stop()

# --- VISTA RÁPIDA DE DATOS ---
st.title("🛜 Multinet NOC Dashboard")
try:
    data = sheet.get_all_records()
    if data:
        st.dataframe(pd.DataFrame(data), use_container_width=True)
    else:
        st.info("La hoja está vacía. Registra datos en el sidebar.")
except Exception as e:
    st.error(f"Error al leer datos: {e}")
