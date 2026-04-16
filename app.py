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
    
    mes_actual_num = datetime.now().month
    mes_seleccionado = st.selectbox("📅 Seleccionar Mes de Análisis", 
                                    ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                                     "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"],
                                    index=mes_actual_num-1)
    
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
        df = pd.DataFrame(records)
        # Guardamos la posición real de la fila en Google Sheets (base 1 + encabezado)
        df['gsheet_id'] = range(2, len(df) + 2)
        df['Fecha_Convertida'] = pd.to_datetime(df['Fecha_Inicio'], dayfirst=True, errors='coerce')
        
        meses_dict = {"Enero":1, "Febrero":2, "Marzo":3, "Abril":4, "Mayo":5, "Junio":6, 
                      "Julio":7, "Agosto":8, "Septiembre":9, "Octubre":10, "Noviembre":11, "Diciembre":12}
        df_mes = df[df['Fecha_Convertida'].dt.month == meses_dict[mes_seleccionado]].copy()

        st.title(f"📊 Dashboard NOC: {mes_seleccionado} {datetime.now().year}")

        if not df_mes.empty:
            # --- SECCIÓN 1: KPIs ---
            k1, k2, k3, k4 = st.columns(4)
            avg_duracion = df_mes['Duracion_Horas'].mean()
            total_impacto = df_mes['Clientes_Afectados'].sum()
            caida_critica = df_mes['Duracion_Horas'].max()
            duracion_total = df_mes['Duracion_Horas'].sum()

            k1.metric("⏱️ MTTR Promedio", f"{avg_duracion:.2f} Horas")
            k2.metric("👥 Impacto Total", f"{int(total_impacto)} Clientes")
            k3.metric("🚨 Caída más Crítica", f"{caida_critica:.2f} Horas")
            k4.metric("⏳ Duración Acumulada", f"{duracion_total:.2f} Horas")

            # --- SECCIÓN 2: GRÁFICAS ---
            st.divider()
            st.subheader("📈 Volumen Mensual de Fallas")
            fig_trend = px.line(df_mes.groupby('Fecha_Convertida').size().reset_index(name='Cant'), 
                                x='Fecha_Convertida', y='Cant', markers=True, template="plotly_dark")
            fig_trend.update_traces(line_color='#0068c9')
            st.plotly_chart(fig_trend, use_container_width=True)

            # --- SECCIÓN 3: BITÁCORA INTERACTIVA ---
            st.divider()
            st.subheader("🔍 Bitácora e Historial")
            
            # Filtro de búsqueda
            busqueda = st.text_input("Filtrar por palabra clave:", "")
            df_display = df_mes.copy()
            if busqueda:
                df_display = df_display[df_display.astype(str).apply(lambda x: x.str.contains(busqueda, case=False)).any(axis=1)]

            # Agregamos columna de selección al inicio
            df_display.insert(0, "Seleccionar", False)
            
            # Configuramos el editor para que solo la columna "Seleccionar" sea editable
            edited_df = st.data_editor(
                df_display.drop(columns=['Fecha_Convertida']),
                column_config={
                    "Seleccionar": st.column_config.CheckboxColumn(help="Selecciona para eliminar", default=False),
                    "gsheet_id": None # Ocultamos el ID técnico
                },
                disabled=[c for c in df_display.columns if c != "Seleccionar"], # Bloquea el resto de columnas
                use_container_width=True,
                hide_index=True,
                key="tabla_bitacora"
            )

            # Lógica de eliminación
            filas_para_eliminar = edited_df[edited_df["Seleccionar"] == True]

            if not filas_para_eliminar.empty:
                st.warning(f"⚠️ Has seleccionado {len(filas_para_eliminar)} registro(s) para eliminar.")
                if st.button("Confirmar Eliminación Permanente"):
                    # Ordenar de mayor a menor para no alterar los índices de las filas restantes al borrar
                    indices_a_borrar = sorted(filas_para_eliminar['gsheet_id'].tolist(), reverse=True)
                    
                    with st.spinner("Eliminando registros..."):
                        for idx in indices_a_borrar:
                            sheet.delete_rows(idx)
                    
                    st.success("Registros eliminados correctamente.")
                    time.sleep(1)
                    st.rerun()
            
            # Botón de Descarga
            csv = df_mes.drop(columns=['gsheet_id', 'Fecha_Convertida']).to_csv(index=False).encode('utf-8')
            st.download_button(label="📥 Descargar Reporte Mensual (CSV)", data=csv, 
                               file_name=f"NOC_Report_{mes_seleccionado}.csv", mime='text/csv')
        else:
            st.info(f"No hay registros encontrados para el mes de {mes_seleccionado}.")
    else:
        st.info("La base de datos está vacía.")
except Exception as e:
    st.error(f"Error al procesar datos: {e}")
