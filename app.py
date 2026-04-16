import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
from datetime import datetime, timedelta
import time

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Multinet NOC Analytics | Enterprise Edition", layout="wide", page_icon="🌐")

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
        st.error(f"Error Crítico de Enlace: {e}")
        return None

sheet = conectar()
if sheet is None: st.stop()

# --- ESTILOS ---
st.markdown("""
    <style>
    div.stButton > button:first-child { background-color: #0068c9; color: white; border-radius: 10px; width: 100%; font-weight: bold; border: none; }
    [data-testid="stMetricLabel"] { color: #808495 !important; font-size: 16px !important; font-weight: 600 !important; }
    [data-testid="stMetricValue"] { color: #ffffff !important; }
    .stAlert { border-radius: 10px; }
    </style>
    """, unsafe_allow_html=True)

# --- SIDEBAR: GESTIÓN OPERATIVA ---
with st.sidebar:
    st.title("🛡️ Centro de Operaciones")
    
    meses_nombres = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                     "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    
    mes_actual_num = datetime.now().month
    mes_seleccionado = st.selectbox("📅 Ciclo de Análisis Mensual", meses_nombres, index=mes_actual_num-1)
    
    st.divider()
    st.header("📋 Reporte de Incidencias")
    with st.form("registro_falla", clear_on_submit=True):
        zona = st.text_input("📍 Localización de Nodo/Zona")
        categoria = st.selectbox("👥 Segmentación de Impacto", ["Red Multinet", "Cliente Corporativo"])
        equipo = st.selectbox("⚙️ Equipamiento Afectado", ["OLT", "RB/Mikrotik", "Switch", "ONU", "Servidor", "Fibra Principal", "Caja NAP"])
        
        c1, c2 = st.columns(2)
        f_i = c1.date_input("🗓️ Inicio Incidente")
        h_i = c2.time_input("🕒 Hora Apertura")
        
        # Lógica de estados desconocidos
        st.write("---")
        desc_estado = st.radio("📉 Estado de Finalización", ["Finalizado con registro exacto", "Sin fecha/hora exacta de cierre"], horizontal=True)
        
        c3, c4 = st.columns(2)
        if desc_estado == "Finalizado con registro exacto":
            f_f = c3.date_input("🗓️ Cierre Incidente")
            h_f = c4.time_input("🕒 Hora Cierre")
            final_f = f_f.strftime("%d/%m/%Y")
            final_h = h_f.strftime("%H:%M:%S")
            dt_i = datetime.combine(f_i, h_i)
            dt_f = datetime.combine(f_f, h_f)
            duracion = round((dt_f - dt_i).total_seconds() / 3600, 2)
        else:
            st.info("⚠️ El incidente se registrará como 'Pendiente de Cierre'.")
            final_f = "N/A"
            final_h = "N/A"
            duracion = 0

        st.write("---")
        clientes = st.number_input("👥 Usuarios Afectados", min_value=0, step=1)
        causa = st.selectbox("🔍 Diagnóstico Causa Raíz", [
            "Corte de Fibra Óptica", 
            "Inestabilidad Suministro Eléctrico", 
            "Desajuste de Configuración", 
            "Vandalismo / Sabotaje", 
            "Degradación de Hardware",
            "Condiciones Atmosféricas Adversas", # Situaciones climáticas
            "Daños por Fauna Sinantrópica"      # Roedores/Ardillas
        ])
        desc = st.text_area("📝 Detalles Técnicos")
        
        if st.form_submit_button("🚀 Sincronizar con Central"):
            nueva_fila = [
                zona, categoria, equipo, f_i.strftime("%d/%m/%Y"), h_i.strftime("%H:%M:%S"), 
                final_f, final_h, int(clientes), causa, desc, duracion
            ]
            sheet.append_row(nueva_fila)
            st.toast("✅ Base de datos actualizada satisfactoriamente")
            time.sleep(1)
            st.rerun()

# --- PROCESAMIENTO Y ANALÍTICA ---
try:
    records = sheet.get_all_records()
    if records:
        df_total = pd.DataFrame(records)
        df_total['gsheet_id'] = range(2, len(df_total) + 2)
        df_total['Fecha_Convertida'] = pd.to_datetime(df_total['Fecha_Inicio'], dayfirst=True, errors='coerce')
        df_total['Mes_Nombre'] = df_total['Fecha_Convertida'].dt.month.map(lambda x: meses_nombres[int(x)-1] if pd.notnull(x) else None)

        df_mes = df_total[df_total['Mes_Nombre'] == mes_seleccionado].copy()

        st.title(f"📈 Inteligencia Operacional NOC: {mes_seleccionado} {datetime.now().year}")

        if not df_mes.empty:
            # --- KPIs ESTRATÉGICOS ---
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("⏱️ MTTR (Media)", f"{df_mes['Duracion_Horas'].mean():.2f} h", help="Mean Time To Repair: Tiempo promedio de resolución de incidentes.")
            k2.metric("👥 Impacto Acumulado", f"{int(df_mes['Clientes_Afectados'].sum())}", help="Total de usuarios que han experimentado degradación del servicio.")
            k3.metric("🚨 Máxima Indisponibilidad", f"{df_mes['Duracion_Horas'].max():.2f} h", help="El incidente de mayor duración registrado en el periodo actual.")
            k4.metric("⏳ Downtime Total", f"{df_mes['Duracion_Horas'].sum():.2f} h", help="Suma total de horas de inactividad de la infraestructura.")

            # --- VISUALIZACIÓN DE ALTO NIVEL ---
            st.divider()
            
            # 1. Tendencia Temporal
            df_trend = df_mes.groupby('Fecha_Convertida').size().reset_index(name='Total_Eventos')
            fig_trend = px.area(df_trend, x='Fecha_Convertida', y='Total_Eventos', 
                                title="📉 <b>Análisis de Estabilidad de Red</b><br><sup>Fluctuación diaria de eventos críticos detectados</sup>",
                                template="plotly_dark")
            fig_trend.update_traces(line_color='#0068c9', fillcolor='rgba(0, 104, 201, 0.2)')
            fig_trend.update_layout(xaxis_title=None, yaxis_title="Volumen de Eventos")
            st.plotly_chart(fig_trend, use_container_width=True)

            # 2. Distribución por Categoría
            fig_pie = px.pie(df_mes, names='Categoria', title="📂 <b>Composición de Cartera Afectada</b><br><sup>Distribución porcentual por segmento de mercado</sup>", 
                             hole=0.6, template="plotly_dark", color_discrete_sequence=['#0068c9', '#ff4b4b'])
            fig_pie.update_traces(textposition='outside', textinfo='percent+label')
            st.plotly_chart(fig_pie, use_container_width=True)
            
            # 3. Top Zonas
            top_zonas = df_mes.groupby('Zona')['Duracion_Horas'].sum().nlargest(5).reset_index()
            fig_bar = px.bar(top_zonas, x='Duracion_Horas', y='Zona', orientation='h',
                             title="📍 <b>Puntos Críticos de Inactividad</b><br><sup>Top 5 sectores con mayor degradación de disponibilidad</sup>",
                             labels={'Duracion_Horas': 'Horas Acumuladas Offline'},
                             text_auto='.2f', template="plotly_dark", color='Duracion_Horas', color_continuous_scale='Blues')
            fig_bar.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig_bar, use_container_width=True)

            # --- BITÁCORA INTELIGENTE (CON RECALCULO DE TIEMPO) ---
            st.divider()
            st.subheader(f"🔍 Auditoría y Gestión de Bitácora: {mes_seleccionado}")
            st.caption("Los cambios en fechas y horas recalcularán automáticamente el tiempo de duración al sincronizar.")
            
            df_display = df_mes.copy()
            df_display.insert(0, "Seleccionar", False)
            
            edited_df = st.data_editor(
                df_display.drop(columns=['Fecha_Convertida', 'Mes_Nombre']),
                column_config={
                    "Seleccionar": st.column_config.CheckboxColumn(default=False), 
                    "gsheet_id": None,
                    "Duracion_Horas": st.column_config.NumberColumn("Duración (h)", disabled=True, format="%.2f")
                },
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="main_editor"
            )

            # Lógica para ELIMINAR
            filas_para_eliminar = edited_df[edited_df["Seleccionar"] == True]
            if not filas_para_eliminar.empty:
                if st.button(f"🗑️ Eliminar permanentemente ({len(filas_para_eliminar)})"):
                    indices = sorted(filas_para_eliminar['gsheet_id'].tolist(), reverse=True)
                    for idx in indices:
                        sheet.delete_rows(idx)
                    st.success("Registros depurados correctamente.")
                    time.sleep(1)
                    st.rerun()

            # Lógica para EDITAR con RECÁLCULO DE TIEMPO
            original_data = df_display.drop(columns=['Fecha_Convertida', 'Mes_Nombre', 'Seleccionar'])
            edited_data = edited_df.drop(columns=['Seleccionar'])
            
            if not original_data.equals(edited_data):
                if st.button("💾 Sincronizar Ediciones"):
                    for i in range(len(original_data)):
                        if not original_data.iloc[i].equals(edited_data.iloc[i]):
                            fila = edited_data.iloc[i].copy()
                            row_idx = int(fila['gsheet_id'])
                            
                            # Lógica de Recálculo de Tiempo Automático al editar
                            try:
                                if str(fila['Fecha_Fin']) != "N/A" and str(fila['Hora_Fin']) != "N/A":
                                    dt_ini = datetime.strptime(f"{fila['Fecha_Inicio']} {fila['Hora_Inicio']}", "%d/%m/%Y %H:%M:%S")
                                    dt_fin = datetime.strptime(f"{fila['Fecha_Fin']} {fila['Hora_Fin']}", "%d/%m/%Y %H:%M:%S")
                                    fila['Duracion_Horas'] = round((dt_fin - dt_ini).total_seconds() / 3600, 2)
                                else:
                                    fila['Duracion_Horas'] = 0
                            except:
                                fila['Duracion_Horas'] = 0

                            # Preparación de datos para GSheets
                            row_values = [str(x) if not isinstance(x, (int, float)) else x for x in fila.drop('gsheet_id').tolist()]
                            sheet.update(f"A{row_idx}:K{row_idx}", [row_values])
                    
                    st.success("✅ Sincronización completa. Tiempos recalculados.")
                    time.sleep(1)
                    st.rerun()
            
            csv_m = df_mes.drop(columns=['gsheet_id', 'Fecha_Convertida', 'Mes_Nombre']).to_csv(index=False).encode('utf-8')
            st.download_button(label=f"📥 Exportar Reporte {mes_seleccionado} (CSV)", data=csv_m, 
                               file_name=f"Reporte_NOC_{mes_seleccionado}.csv", mime='text/csv')

        else:
            st.info(f"No se detectan registros operativos para el ciclo de {mes_seleccionado}.")

        # --- ARCHIVO HISTÓRICO ---
        st.divider()
        st.header("📂 Repositorio de Registros Históricos")
        meses_con_datos = [m for m in meses_nombres if m in df_total['Mes_Nombre'].unique()]
        otros_meses = [m for m in meses_con_datos if m != mes_seleccionado]
        
        if otros_meses:
            for mes in otros_meses:
                with st.expander(f"📁 Consolidado Mensual: {mes}"):
                    df_hist = df_total[df_total['Mes_Nombre'] == mes].drop(columns=['gsheet_id', 'Fecha_Convertida', 'Mes_Nombre'])
                    st.dataframe(df_hist, use_container_width=True, hide_index=True)
                    csv_h = df_hist.to_csv(index=False).encode('utf-8')
                    st.download_button(label=f"Exportar {mes}", data=csv_h, 
                                       file_name=f"Historico_{mes}.csv", mime='text/csv', key=f"btn_{mes}")
        else:
            st.info("No se dispone de datos en periodos anteriores.")

    else:
        st.info("La base de datos se encuentra íntegra pero vacía.")
except Exception as e:
    st.error(f"Error en el Procesamiento de Datos: {e}")
