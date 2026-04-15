import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
from datetime import datetime
import time

# --- CONFIGURACIÓN DE CONEXIÓN (CORREGIDA PARA CLOUD) ---
def conectar():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Intentamos leer desde los Secrets de Streamlit (Para la Nube)
    if "gcp_service_account" in st.secrets:
        creds_dict = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    else:
        # Esto solo funcionará en tu PC local
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        
    client = gspread.authorize(creds)
    return client.open("Dashboard_ISP").sheet1

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Multinet NOC Analytics", layout="wide", page_icon="💻")

# Estilo CSS personalizado
st.markdown("""
    <style>
    div.stButton > button:first-child {
        background-color: #0068c9;
        color: white;
        border-radius: 10px;
        width: 100%;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)

# Conexión inicial
try:
    sheet = conectar()
except Exception as e:
    st.error(f"❌ Error de conexión con Google Sheets: {e}")
    st.stop()

st.title("🛜 Multinet NOC: Gestión Avanzada de Incidentes")

# --- SIDEBAR: ENTRADA DE DATOS ---
with st.sidebar:
    st.header("🌐 Entrada de Datos")
    with st.form("registro_falla", clear_on_submit=True):
        zona = st.text_input("📍 Zona / Ubicación")
        categoria = st.selectbox("📁 Categoría", ["Red Multinet", "Cliente Corporativo"])
        equipo = st.selectbox("🛠️ Equipo Afectado", [
            "OLT", "RB/Mikrotik", "Switch", "ONU", "Servidor", 
            "Fibra Principal", "Antenas Ubiquiti", "Sistema de Vauchers", "Caja NAP"
        ])
        
        c1, c2 = st.columns(2)
        f_inicio = c1.date_input("🗓️ Fecha Inicio")
        h_inicio = c2.time_input("🕒 Hora Inicio")
        
        c3, c4 = st.columns(2)
        f_fin = c3.date_input("🗓️ Fecha Fin")
        h_fin = c4.time_input("🕒 Hora Fin")
        
        clientes = st.number_input("👥 Clientes Afectados", min_value=0, step=1)
        causa = st.selectbox("🔍 Causa Raíz", ["Corte Fibra", "Falla Eléctrica", "Configuración", "Vandalismo", "Hardware"])
        desc = st.text_area("📝 Descripción")
        
        btn = st.form_submit_button("Guardar Registro")
        
        if btn:
            with st.status("🚀 Sincronizando...", expanded=False):
                dt_i = datetime.combine(f_inicio, h_inicio)
                dt_f = datetime.combine(f_fin, h_fin)
                duracion = round((dt_f - dt_i).total_seconds() / 3600, 2)
                
                nueva_fila = [
                    zona, categoria, equipo, f_inicio.strftime("%d/%m/%Y"), h_inicio.strftime("%H:%M:%S"), 
                    f_fin.strftime("%d/%m/%Y"), h_fin.strftime("%H:%M:%S"), int(clientes), causa, desc, duracion
                ]
                sheet.append_row(nueva_fila)
                st.toast("✅ ¡Información guardada!")
                time.sleep(1)
                st.rerun()

# --- DASHBOARD PRINCIPAL ---
try:
    records = sheet.get_all_records()
    if records:
        df = pd.DataFrame(records)
        
        # Limpieza técnica
        df['Duracion_Horas'] = pd.to_numeric(df['Duracion_Horas'], errors='coerce').fillna(0)
        df['Clientes_Afectados'] = pd.to_numeric(df['Clientes_Afectados'], errors='coerce').fillna(0)
        df['Fecha_Convertida'] = pd.to_datetime(df['Fecha_Inicio'], dayfirst=True, errors='coerce')
        df = df.dropna(subset=['Fecha_Convertida'])

        # KPIs
        st.subheader("📊 Indicadores Críticos (KPIs)")
        k1, k2, k3, k4 = st.columns(4)
        
        k1.metric("⏱️ MTTR Promedio", f"{df['Duracion_Horas'].mean():.2f} hrs")
        k2.metric("👥 Impacto Total", f"{int(df['Clientes_Afectados'].sum())} Cli")
        k3.metric("📉 Total Incidentes", len(df))
        k4.metric("🚨 Máxima Caída", f"{df['Duracion_Horas'].max():.1f} hrs")

        # Gráficas
        st.divider()
        st.subheader("📈 Tendencia Diaria")
        df_trend = df.groupby('Fecha_Convertida').size().reset_index(name='Cantidad')
        st.plotly_chart(px.area(df_trend, x='Fecha_Convertida', y='Cantidad', title="Fallas por día"), use_container_width=True)

        st.divider()
        c_left, c_right = st.columns(2)
        
        with c_left:
            st.plotly_chart(px.pie(df, names="Categoria", hole=0.5, title="Distribución por Categoría"), use_container_width=True)
        
        with c_right:
            st.plotly_chart(px.bar(df, x="Equipo_Afectado", y="Duracion_Horas", color="Causa_Raiz", title="Inactividad por Equipo"), use_container_width=True)

        # Bitácora
        st.divider()
        with st.expander("🔍 Ver Bitácora Completa"):
            st.dataframe(df, use_container_width=True)
            
    else:
        st.info("💡 La base de datos está vacía.")

except Exception as e:
    st.error(f"⚠️ Error al cargar datos: {e}")
