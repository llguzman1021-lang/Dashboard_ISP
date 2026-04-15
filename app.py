import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
from datetime import datetime
import time

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Multinet NOC Analytics", layout="wide", page_icon="💻")

# --- FUNCIÓN DE CONEXIÓN (MANTENIENDO LO QUE FUNCIONA) ---
def conectar():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds_info = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(dict(creds_info), scope)
        client = gspread.authorize(creds)
        return client.open("Dashboard_ISP").sheet1
    except Exception as e:
        st.error(f"❌ Error de conexión: {e}")
        st.stop()

sheet = conectar()

# --- ESTILO CSS PARA BOTONES ---
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
        
        # Limpieza y conversión de datos
        df['Duracion_Horas'] = pd.to_numeric(df['Duracion_Horas'], errors='coerce').fillna(0)
        df['Clientes_Afectados'] = pd.to_numeric(df['Clientes_Afectados'], errors='coerce').fillna(0)
        df['Fecha_Convertida'] = pd.to_datetime(df['Fecha_Inicio'], dayfirst=True, errors='coerce')
        df = df.dropna(subset=['Fecha_Convertida'])

        # --- SECCIÓN 1: KPIs ---
        st.subheader("📊 Indicadores Críticos (KPIs)")
        k1, k2, k3, k4 = st.columns(4)
        
        mttr = df['Duracion_Horas'].mean()
        total_clientes = df['Clientes_Afectados'].sum()
        total_fallas = len(df)
        max_caida = df['Duracion_Horas'].max()

        k1.metric("⏱️ MTTR Promedio", f"{mttr:.2f} hrs")
        k2.metric("👥 Impacto Total", f"{int(total_clientes)} Cli")
        k3.metric("📉 Total Incidentes", total_fallas)
        k4.metric("🚨 Máxima Caída", f"{max_caida:.1f} hrs")

        # --- SECCIÓN 2: GRÁFICAS ---
        st.divider()
        st.subheader("📈 Tendencia Diaria de Incidentes")
        df_trend = df.groupby('Fecha_Convertida').size().reset_index(name='Cantidad')
        fig_trend = px.area(df_trend, x='Fecha_Convertida', y='Cantidad', 
                            line_shape="spline", color_discrete_sequence=['#FF4B4B'],
                            title="Volumen de fallas detectadas por día")
        st.plotly_chart(fig_trend, use_container_width=True)

        st.divider()
        col_pie, col_bar = st.columns(2)
        
        with col_pie:
            st.subheader("🍩 Distribución por Categoría")
            fig_pie = px.pie(df, names="Categoria", hole=0.5, 
                             color_discrete_sequence=px.colors.qualitative.Safe)
            st.plotly_chart(fig_pie, use_container_width=True)
            
        with col_bar:
            st.subheader("🛜 Inactividad por Equipo")
            fig_bar = px.bar(df, x="Equipo_Afectado", y="Duracion_Horas", color="Causa_Raiz", 
                             barmode="group", text_auto='.1f')
            st.plotly_chart(fig_bar, use_container_width=True)

        # --- SECCIÓN 3: BITÁCORA ---
        st.divider()
        with st.expander("🔍 Ver Bitácora Completa (Detalle de Excel)"):
            st.dataframe(df.drop(columns=['Fecha_Convertida']), use_container_width=True)
            
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Descargar Reporte CSV", data=csv, 
                               file_name=f"reporte_multinet_{datetime.now().strftime('%d_%m_%Y')}.csv", 
                               mime="text/csv")
            
    else:
        st.info("💡 La base de datos está vacía. Registra el primer incidente en el panel lateral.")

except Exception as e:
    st.error(f"⚠️ Error de visualización: {e}")
