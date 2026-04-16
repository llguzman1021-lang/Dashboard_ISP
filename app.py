import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
from datetime import datetime
import time

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Multinet NOC Analytics", layout="wide", page_icon="🌐")

# --- CONEXIÓN SEGURA ---
@st.cache_resource(ttl=300)
def conectar():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds_info = dict(st.secrets["gcp_service_account"])
        creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
        client = gspread.authorize(creds)
        return client.open("Dashboard_ISP").sheet1
    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return None

sheet = conectar()
if sheet is None: st.stop()

# --- ESTILOS ---
st.markdown("""
    <style>
    div.stButton > button:first-child { background-color: #0068c9; color: white; border-radius: 10px; width: 100%; font-weight: bold; }
    [data-testid="stMetricLabel"] { color: #ffffff !important; }
    .stAlert { border-radius: 10px; }
    </style>
    """, unsafe_allow_html=True)

# --- SIDEBAR: CONFIGURACIÓN Y ENTRADA ---
with st.sidebar:
    st.title("⚙️ Panel de Control")
    
    meses_nombres = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                     "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    
    mes_actual_num = datetime.now().month
    mes_seleccionado = st.selectbox("📅 Seleccionar Mes de Análisis", meses_nombres, index=mes_actual_num-1)
    
    st.divider()
    st.header("📝 Registro de Incidente")
    with st.form("registro_falla", clear_on_submit=True):
        zona = st.text_input("📍 Zona")
        categoria = st.selectbox("📂 Categoría", ["Red Multinet", "Cliente Corporativo"])
        equipo = st.selectbox("🔧 Equipo", ["OLT", "RB/Mikrotik", "Switch", "ONU", "Servidor", "Fibra Principal", "Caja NAP"])
        
        c1, c2 = st.columns(2)
        f_i = c1.date_input("🗓️ Fecha Inicio")
        h_i = c2.time_input("🕒 Hora Inicio")
        
        c3, c4 = st.columns(2)
        f_f = c3.date_input("🗓️ Fecha Fin")
        h_f = c4.time_input("🕒 Hora Fin")
        
        sin_hora = st.checkbox("❓ Sin hora/fecha exacta de fin")
        clientes = st.number_input("👥 Cantidad Clientes", min_value=0, step=1)
        sin_clientes = st.checkbox("❓ Sin dato exacto de clientes")
        causa = st.selectbox("🔎 Causa Raíz", ["Corte Fibra", "Falla Eléctrica", "Configuración", "Vandalismo", "Hardware"])
        desc = st.text_area("📄 Descripción")
        
        if st.form_submit_button("Guardar en Bitácora"):
            dt_i = datetime.combine(f_i, h_i)
            dt_f = datetime.combine(f_f, h_f)
            duracion = round((dt_f - dt_i).total_seconds() / 3600, 2) if not sin_hora else 0
            
            nueva_fila = [
                zona, categoria, equipo, f_i.strftime("%d/%m/%Y"), h_i.strftime("%H:%M:%S"), 
                f_f.strftime("%d/%m/%Y") if not sin_hora else "N/A", 
                h_f.strftime("%H:%M:%S") if not sin_hora else "N/A", 
                int(clientes) if not sin_clientes else 0, 
                causa, desc, duracion
            ]
            sheet.append_row(nueva_fila)
            st.toast("✅ Registro Exitoso")
            time.sleep(1)
            st.rerun()

# --- PROCESAMIENTO DE DATOS ---
try:
    records = sheet.get_all_records()
    if records:
        df_total = pd.DataFrame(records)
        # ID para gestión de filas en Google Sheets
        df_total['gsheet_id'] = range(2, len(df_total) + 2)
        df_total['Fecha_Convertida'] = pd.to_datetime(df_total['Fecha_Inicio'], dayfirst=True, errors='coerce')
        # Extraer nombre del mes para agrupamiento
        df_total['Mes_Nombre'] = df_total['Fecha_Convertida'].dt.month.map(lambda x: meses_nombres[int(x)-1] if pd.notnull(x) else None)

        # 1. Filtrado para el Dashboard Principal (Mes seleccionado)
        df_mes = df_total[df_total['Mes_Nombre'] == mes_seleccionado].copy()

        st.title(f"📊 Dashboard NOC: {mes_seleccionado} {datetime.now().year}")

        if not df_mes.empty:
            # --- KPIs ---
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("⏱️ MTTR Promedio", f"{df_mes['Duracion_Horas'].mean():.2f} h")
            k2.metric("👥 Impacto Total", f"{int(df_mes['Clientes_Afectados'].sum())} Clientes")
            k3.metric("🚨 Máx Caída", f"{df_mes['Duracion_Horas'].max():.2f} h")
            k4.metric("⏳ Acumulado", f"{df_mes['Duracion_Horas'].sum():.2f} h")

            # --- GRÁFICAS ---
            st.divider()
            fig_trend = px.line(df_mes.groupby('Fecha_Convertida').size().reset_index(name='Cant'), 
                                x='Fecha_Convertida', y='Cant', title="Tendencia de Fallas", markers=True, template="plotly_dark")
            fig_trend.update_traces(line_color='#0068c9')
            st.plotly_chart(fig_trend, use_container_width=True)

            # --- BITÁCORA DEL MES (CON ELIMINACIÓN) ---
            st.subheader(f"🔍 Gestión de Registros: {mes_seleccionado}")
            
            df_display = df_mes.copy()
            df_display.insert(0, "Seleccionar", False)
            
            # Editor configurado: solo permite seleccionar el Checkbox, no editar datos ni agregar filas
            edited_df = st.data_editor(
                df_display.drop(columns=['Fecha_Convertida', 'Mes_Nombre']),
                column_config={
                    "Seleccionar": st.column_config.CheckboxColumn(default=False), 
                    "gsheet_id": None
                },
                disabled=[c for c in df_display.columns if c != "Seleccionar"],
                use_container_width=True,
                hide_index=True,
                num_rows="fixed", # Impide agregar filas desde la tabla
                key="main_editor"
            )

            filas_para_eliminar = edited_df[edited_df["Seleccionar"] == True]
            if not filas_para_eliminar.empty:
                st.warning(f"⚠️ Seleccionaste {len(filas_para_eliminar)} fila(s).")
                if st.button(f"Confirmar Eliminación de {len(filas_para_eliminar)} registros"):
                    indices = sorted(filas_para_eliminar['gsheet_id'].tolist(), reverse=True)
                    for idx in indices:
                        sheet.delete_rows(idx)
                    st.success("Registros eliminados.")
                    time.sleep(1)
                    st.rerun()
            
            # Botón de Descarga para el mes actual
            csv_m = df_mes.drop(columns=['gsheet_id', 'Fecha_Convertida', 'Mes_Nombre']).to_csv(index=False).encode('utf-8')
            st.download_button(label=f"📥 Descargar CSV {mes_seleccionado}", data=csv_m, 
                               file_name=f"Reporte_{mes_seleccionado}.csv", mime='text/csv')

        else:
            st.info(f"No hay registros encontrados para el mes de {mes_seleccionado}.")

        # --- SECCIÓN: ARCHIVO HISTÓRICO (OTROS MESES) ---
        st.divider()
        st.header("📂 Histórico de Otros Meses")
        
        # Filtrar meses que tienen datos pero no son el seleccionado actualmente
        meses_con_datos = [m for m in meses_nombres if m in df_total['Mes_Nombre'].unique()]
        otros_meses = [m for m in meses_con_datos if m != mes_seleccionado]
        
        if otros_meses:
            for mes in otros_meses:
                with st.expander(f"📁 Bitácora de {mes}"):
                    # Mostrar tabla solo lectura
                    df_hist = df_total[df_total['Mes_Nombre'] == mes].drop(columns=['gsheet_id', 'Fecha_Convertida', 'Mes_Nombre'])
                    st.dataframe(df_hist, use_container_width=True, hide_index=True)
                    
                    # Descarga específica por mes
                    csv_h = df_hist.to_csv(index=False).encode('utf-8')
                    st.download_button(label=f"Descargar CSV {mes}", data=csv_h, 
                                       file_name=f"Historico_{mes}.csv", mime='text/csv', key=f"btn_{mes}")
        else:
            st.info("No hay registros históricos en otros meses.")

    else:
        st.info("La base de datos está vacía.")
except Exception as e:
    st.error(f"Error al procesar datos: {e}")
