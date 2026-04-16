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
    </style>
    """, unsafe_allow_html=True)

# --- SIDEBAR: CONFIGURACIÓN Y ENTRADA ---
with st.sidebar:
    st.title("⚙️ Panel de Control")
    
    # Filtro de Mes para el Dashboard
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
        # Mantener el índice original para poder borrar correctamente en Google Sheets
        df['gsheet_row'] = df.index + 2  # +2 porque gspread es base 1 y la fila 1 es el encabezado
        
        df['Fecha_Convertida'] = pd.to_datetime(df['Fecha_Inicio'], dayfirst=True, errors='coerce')
        
        # Filtrado por Mes Seleccionado
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

            # --- SECCIÓN 2: GRÁFICAS VERTICALES ---
            st.divider()
            st.subheader("📈 Volumen Mensual de Fallas")
            fig_trend = px.line(df_mes.groupby('Fecha_Convertida').size().reset_index(name='Cant'), 
                                x='Fecha_Convertida', y='Cant', markers=True, template="plotly_dark")
            fig_trend.update_traces(line_color='#0068c9')
            st.plotly_chart(fig_trend, use_container_width=True)

            st.subheader("📂 Distribución por Categoría")
            fig_pie = px.pie(df_mes, names="Categoria", hole=0.4, template="plotly_dark", color_discrete_sequence=px.colors.qualitative.Pastel)
            st.plotly_chart(fig_pie, use_container_width=True)

            st.subheader("🛠️ Inactividad Acumulada por Equipo")
            fig_bar = px.bar(df_mes.groupby('Equipo_Afectado')['Duracion_Horas'].sum().reset_index(), 
                             x="Equipo_Afectado", y="Duracion_Horas", template="plotly_dark", color="Equipo_Afectado")
            st.plotly_chart(fig_bar, use_container_width=True)

            # --- SECCIÓN 3: BITÁCORA INTERACTIVA CON OPCIÓN DE ELIMINAR ---
            st.divider()
            st.subheader("🔍 Bitácora e Historial")
            st.info("💡 Para eliminar: Selecciona la fila y presiona la tecla 'Supr' (Delete) o usa el icono de papelera. Luego presiona el botón 'Confirmar Cambios'.")
            
            # Filtro de búsqueda interactivo
            busqueda = st.text_input("Filtrar por Zona, Equipo o Causa:", "")
            df_mostrar = df_mes.copy()
            if busqueda:
                df_mostrar = df_mostrar[df_mostrar.astype(str).apply(lambda x: x.str.contains(busqueda, case=False)).any(axis=1)]

            # Editor de datos interactivo
            # No mostramos la columna técnica 'gsheet_row' ni 'Fecha_Convertida'
            columnas_visibles = [c for c in df_mostrar.columns if c not in ['gsheet_row', 'Fecha_Convertida']]
            
            edited_df = st.data_editor(
                df_mostrar[ ['gsheet_row'] + columnas_visibles ], 
                column_config={"gsheet_row": None}, # Ocultar columna de ID técnico
                use_container_width=True,
                num_rows="dynamic", # Permite borrar filas
                key="editor_bitacora"
            )

            # Lógica para confirmar eliminación
            if st.button("⚠️ Confirmar Cambios / Eliminar Filas"):
                # Identificar filas que fueron borradas comparando el DF original con el editado
                rows_remaining = edited_df['gsheet_row'].tolist()
                rows_to_delete = [r for r in df_mostrar['gsheet_row'].tolist() if r not in rows_remaining]
                
                if rows_to_delete:
                    # Ordenar de mayor a menor para no arruinar los índices al borrar
                    for row_idx in sorted(rows_to_delete, reverse=True):
                        sheet.delete_rows(row_idx)
                    st.success(f"Se eliminaron {len(rows_to_delete)} registros correctamente.")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.warning("No se detectaron cambios para eliminar.")
            
            # Botón de Descarga
            csv = df_mes.drop(columns=['gsheet_row', 'Fecha_Convertida']).to_csv(index=False).encode('utf-8')
            st.download_button(label="📥 Descargar Reporte Mensual (CSV)", data=csv, 
                               file_name=f"NOC_Report_{mes_seleccionado}.csv", mime='text/csv')
        else:
            st.info(f"No hay registros encontrados para el mes de {mes_seleccionado}.")
    else:
        st.info("La base de datos está vacía.")
except Exception as e:
    st.error(f"Error al procesar datos: {e}")
