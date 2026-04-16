import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
from datetime import datetime
import time

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Multinet NOC Analytics | Enterprise Operations", layout="wide", page_icon="🌐")

# --- CONEXIÓN SEGURA ---
@st.cache_resource(ttl=300)
def conectar():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds_info = dict(st.secrets["gcp_service_account"])
        creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
        client = gspread.authorize(creds)
        # Se asume que la hoja tiene las columnas correctas incluyendo 'Servicio' y 'Conocimiento_Tiempos'
        return client.open("Dashboard_ISP").sheet1
    except Exception as e:
        st.error(f"Error de enlace con la base de datos central: {e}")
        return None

sheet = conectar()
if sheet is None: st.stop()

# --- ESTILOS PROFESIONALES ---
st.markdown("""
    <style>
    /* Estilo global de botones Streamlit (azul Multinet) */
    div.stButton > button:first-child { 
        background-color: #0068c9; 
        color: white; 
        border-radius: 8px; 
        width: 100%; 
        font-weight: 600; 
        border: none;
        padding: 0.5rem 1rem;
        transition: all 0.2s;
    }
    div.stButton > button:first-child:hover {
        background-color: #0056a3;
        border: none;
        color: white;
    }
    /* Estilo de etiquetas de KPIs */
    [data-testid="stMetricLabel"] { 
        color: #808495 !important; 
        font-size: 15px !important; 
        font-weight: 500 !important; 
    }
    /* Estilo de valores de KPIs */
    [data-testid="stMetricValue"] { 
        color: #ffffff !important; 
        font-size: 32px !important;
        font-weight: 700 !important;
    }
    /* Redondear alertas y markdown */
    .stAlert, .stMarkdown { border-radius: 8px; }
    </style>
    """, unsafe_allow_html=True)

# --- SIDEBAR: GESTIÓN OPERATIVA ---
with st.sidebar:
    st.title("🛡️ Centro de Operaciones")
    st.caption("Panel de Control Multinet | v2.5")
    
    meses_nombres = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                     "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    
    mes_actual_num = datetime.now().month
    mes_seleccionado = st.selectbox("📅 Ciclo de Análisis Mensual", meses_nombres, index=mes_actual_num-1)
    
    st.divider()
    st.header("📋 Reporte de Incidencias NOC")
    with st.form("registro_falla", clear_on_submit=True):
        zona = st.text_input("📍 Localización del Nodo/Zona")
        
        c_serv1, c_serv2 = st.columns([2, 3])
        servicio = c_serv1.selectbox("📡 Servicio Afectado", ["Internet", "Cable TV (CATV)", "IPTV (Mnet+)"])
        categoria = c_serv2.selectbox("👥 Segmentación de Impacto", ["Red Multinet (Troncal)", "Cliente Corporativo"])
        
        equipo = st.selectbox("⚙️ Equipamiento Afectado", ["OLT", "RB/Mikrotik", "Switch", "ONU", "Servidor", "Fibra Principal", "Caja NAP"])
        
        st.write("---")
        st.write("⏱️ **Ventana Temporal de Inicio**")
        c1, c2 = st.columns(2)
        f_i = c1.date_input("🗓️ Fecha de Inicio")
        h_i = c2.time_input("🕒 Hora de Apertura")
        
        st.write("---")
        st.write("📉 **Estado de Cierre (Cálculo de Tiempos)**")
        
        c_c1, c_c2 = st.columns(2)
        conoce_f_f = c_c1.radio("🗓️ ¿Conoce Fecha de Cierre?", ["Sí", "No"], horizontal=True)
        conoce_h_f = c_c2.radio("🕒 ¿Conoce Hora de Cierre?", ["Sí", "No"], horizontal=True)
        
        # Lógica de estados desconocidos profesional
        st.info("ℹ️ Si selecciona 'No' en fecha u hora, el sistema registrará 'N/A' y la duración como 0h.")
        
        c3, c4 = st.columns(2)
        final_f = "N/A"
        final_h = "N/A"
        duracion = 0
        desc_conocimiento = "Total" # Por defecto

        if conoce_f_f == "Sí" and conoce_h_f == "Sí":
            f_f = c3.date_input("🗓️ Fecha de Cierre")
            h_f = c4.time_input("🕒 Hora de Cierre")
            final_f = f_f.strftime("%d/%m/%Y")
            final_h = h_f.strftime("%H:%M:%S")
            desc_conocimiento = "Total"
            
            # Cálculo de Duración Profesional
            try:
                dt_i = datetime.combine(f_i, h_i)
                dt_f = datetime.combine(f_f, h_f)
                duracion = round((dt_f - dt_i).total_seconds() / 3600, 2)
                if duracion < 0:
                    st.error("Error: La fecha/hora de cierre no puede ser anterior a la de inicio.")
                    duracion = 0
                    final_f, final_h, desc_conocimiento = "N/A", "N/A", "N/A"
            except:
                duracion = 0
                final_f, final_h, desc_conocimiento = "N/A", "N/A", "N/A"
        
        elif conoce_f_f == "Sí" and conoce_h_f == "No":
            f_f = c3.date_input("🗓️ Fecha de Cierre")
            final_f = f_f.strftime("%d/%m/%Y")
            final_h = "N/A"
            duracion = 0 # No se puede calcular sin hora
            desc_conocimiento = "Parcial (Solo Fecha)"

        elif conoce_f_f == "No" and conoce_h_f == "Sí":
            h_f = c4.time_input("🕒 Hora de Cierre")
            final_f = "N/A"
            final_h = h_f.strftime("%H:%M:%S")
            duracion = 0 # No se puede calcular sin fecha
            desc_conocimiento = "Parcial (Solo Hora)"
        
        else:
            final_f = "N/A"
            final_h = "N/A"
            duracion = 0
            desc_conocimiento = "Ninguno"

        st.write("---")
        clientes = st.number_input("👥 Usuarios/Clientes Afectados", min_value=0, step=1)
        causa = st.selectbox("🔍 Diagnóstico Causa Raíz", [
            "Corte de Fibra Óptica", 
            "Inestabilidad Suministro Eléctrico", 
            "Desajuste de Configuración", 
            "Vandalismo / Sabotaje", 
            "Degradación de Hardware",
            "Condiciones Atmosféricas Adversas",
            "Daños por Fauna Sinantrópica"
        ])
        desc = st.text_area("📝 Detalles Técnicos / Descripción")
        
        if st.form_submit_button("Guardar Registro Operativo"):
            # Se asume que el orden de columnas en GSheets es:
            # Zona, Servicio, Categoría, Equipo, Fecha_Inicio, Hora_Inicio, Fecha_Fin, Hora_Fin, Clientes_Afectados, Causa_Raiz, Descripción, Duracion_Horas, Conocimiento_Tiempos
            nueva_fila = [
                zona, servicio, categoria, equipo, f_i.strftime("%d/%m/%Y"), h_i.strftime("%H:%M:%S"), 
                final_f, final_h, int(clientes), causa, desc, duracion, desc_conocimiento
            ]
            sheet.append_row(nueva_fila)
            st.toast("✅ Base de datos operativa actualizada satisfactoriamente")
            time.sleep(1)
            st.rerun()

# --- PROCESAMIENTO Y ANALÍTICA DE DATOS ---
try:
    records = sheet.get_all_records()
    if records:
        df_total = pd.DataFrame(records)
        df_total['gsheet_id'] = range(2, len(df_total) + 2)
        df_total['Fecha_Convertida'] = pd.to_datetime(df_total['Fecha_Inicio'], dayfirst=True, errors='coerce')
        df_total['Mes_Nombre'] = df_total['Fecha_Convertida'].dt.month.map(lambda x: meses_nombres[int(x)-1] if pd.notnull(x) else None)

        # Filtrado para el Dashboard Principal (Mes seleccionado)
        df_mes = df_total[df_total['Mes_Nombre'] == mes_seleccionado].copy()

        st.title(f"📊 Dashboard Operacional NOC: {mes_seleccionado} {datetime.now().year}")

        if not df_mes.empty:
            # --- KPIs ESTRATÉGICOS ---
            k_mttr, k_imp, k_max, k_down = st.columns(4)
            # Cálculo profesional de MTTR (Mean Time To Repair) solo sobre registros con tiempos totales
            df_mttr = df_mes[df_mes['Duracion_Horas'] > 0]
            avg_mttr = df_mttr['Duracion_Horas'].mean() if not df_mttr.empty else 0
            
            k_mttr.metric("⏱️ MTTR (Promedio)", f"{avg_mttr:.2f} h", help="Mean Time To Repair: Tiempo promedio de resolución para incidentes con registros completos.")
            k_imp.metric("👥 Impacto Acumulado", f"{int(df_mes['Clientes_Afectados'].sum())}", help="Total de usuarios/clientes afectados por incidencias en el periodo actual.")
            k_max.metric("🚨 Máxima Indisponibilidad", f"{df_mes['Duracion_Horas'].max():.2f} h", help="El incidente de mayor duración registrado en el mes.")
            k_down.metric("⏳ Downtime Total", f"{df_mes['Duracion_Horas'].sum():.2f} h", help="Suma total de horas de inactividad de la infraestructura de red.")

            # Gráfica de Composición de Servicio Afectado (KPI Visual)
            st.write("---")
            df_serv = df_mes.groupby('Servicio').size().reset_index(name='Total_Eventos')
            fig_serv_kpi = px.bar(df_serv, x='Total_Eventos', y='Servicio', orientation='h',
                                title="📡 <b>Composición por Servicio Afectado</b><br><sup>Distribución operativa de incidencias por tipo de servicio</sup>",
                                labels={'Total_Eventos': 'Total de Eventos NOC', 'Servicio': ''},
                                text_auto=True, template="plotly_dark", color='Servicio', color_discrete_sequence=['#0068c9', '#ff9f43', '#27ae60'])
            fig_serv_kpi.update_layout(showlegend=False, height=250, margin=dict(l=0, r=0, t=60, b=0))
            fig_serv_kpi.update_traces(textposition='inside', textfont_size=14, marker_line_width=0)
            st.plotly_chart(fig_serv_kpi, use_container_width=True)


            # --- VISUALIZACIÓN DE ALTO NIVEL ---
            st.divider()
            
            # 1. Tendencia Temporal
            df_trend = df_mes.groupby('Fecha_Convertida').size().reset_index(name='Total_Eventos')
            fig_trend = px.area(df_trend, x='Fecha_Convertida', y='Total_Eventos', 
                                title="📊 <b>Análisis de Estabilidad de Red (Día a Día)</b><br><sup>Fluctuación diaria del volumen total de incidentes operativos</sup>",
                                labels={'Fecha_Convertida': 'Fecha del Evento', 'Total_Eventos': 'Volumen de Eventos'},
                                template="plotly_dark")
            fig_trend.update_traces(line_color='#0068c9', fillcolor='rgba(0, 104, 201, 0.2)')
            st.plotly_chart(fig_trend, use_container_width=True)

            # 2. Distribución por Categoría de Cliente (Ahora uno sobre otro)
            fig_pie = px.pie(df_mes, names='Categoria', title="📂 <b>Composición de Cartera Afectada</b><br><sup>Distribución porcentual de incidentes por segmento de mercado</sup>", 
                            hole=0.6, template="plotly_dark", color_discrete_sequence=['#0068c9', '#ff4b4b'])
            fig_pie.update_traces(textposition='outside', textinfo='percent+label', textfont_size=13)
            fig_pie.update_layout(showlegend=False, margin=dict(l=0, r=0, t=70, b=0))
            st.plotly_chart(fig_pie, use_container_width=True)
            
            # 3. Top Zonas Críticas (Cambiado a icono de tendencia profesional)
            top_zonas = df_mes.groupby('Zona')['Duracion_Horas'].sum().nlargest(5).reset_index()
            top_zonas.columns = ['Zona', 'Horas Offline']
            fig_bar = px.bar(top_zonas, x='Horas Offline', y='Zona', orientation='h',
                             title="📈 <b>Puntos Críticos de Indisponibilidad</b><br><sup>Top 5 sectores geográficos con mayor degradación acumulada de servicio</sup>",
                             labels={'Horas Offline': 'Total Horas de Indisponibilidad'},
                             text_auto='.2f', template="plotly_dark", color='Horas Offline', color_continuous_scale='Blues')
            fig_bar.update_layout(coloraxis_showscale=False, yaxis={'categoryorder':'total ascending'}, margin=dict(l=0, r=0, t=70, b=0))
            fig_bar.update_traces(marker_line_width=0, textfont_size=13)
            st.plotly_chart(fig_bar, use_container_width=True)

            # --- BITÁCORA INTELIGENTE (CON EDICIÓN Y RECALCULO DE TIEMPO) ---
            st.divider()
            st.subheader(f"🔍 Auditoría y Gestión de Bitácora Operativa: {mes_seleccionado}")
            st.caption("ℹ️ Los cambios en fechas, horas o tipo de conocimiento recalcularán automáticamente la duración al sincronizar.")
            
            df_display = df_mes.copy()
            df_display.insert(0, "Seleccionar", False)
            
            # Definir opciones para columnas editables profesionales
            conoce_opciones = ["Total", "Parcial (Solo Fecha)", "Parcial (Solo Hora)", "Ninguno"]
            servicio_opciones = ["Internet", "Cable TV (CATV)", "IPTV (Mnet+)"]

            edited_df = st.data_editor(
                df_display.drop(columns=['Fecha_Convertida', 'Mes_Nombre']),
                column_config={
                    "Seleccionar": st.column_config.CheckboxColumn(default=False), 
                    "gsheet_id": None,
                    "Servicio": st.column_config.SelectboxColumn("Servicio", options=servicio_opciones, required=True),
                    "Fecha_Inicio": st.column_config.TextColumn("Fecha Inicio", help="DD/MM/YYYY"),
                    "Hora_Inicio": st.column_config.TextColumn("Hora Inicio", help="HH:MM:SS"),
                    "Fecha_Fin": st.column_config.TextColumn("Fecha Cierre", help="DD/MM/YYYY o N/A"),
                    "Hora_Fin": st.column_config.TextColumn("Hora Cierre", help="HH:MM:SS o N/A"),
                    "Duracion_Horas": st.column_config.NumberColumn("Duración (h)", disabled=True, format="%.2f"),
                    "Conocimiento_Tiempos": st.column_config.SelectboxColumn("Tipo Conocimiento", options=conoce_opciones, required=True, help="Define cómo se calcula el tiempo")
                },
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="main_editor"
            )

            # Lógica para ELIMINAR
            filas_para_eliminar = edited_df[edited_df["Seleccionar"] == True]
            if not filas_para_eliminar.empty:
                st.warning(f"⚠️ Atención: Está a punto de eliminar permanentemente {len(filas_para_eliminar)} registro(s).")
                if st.button(f"🗑️ Confirmar Depuración de Datos ({len(filas_para_eliminar)})"):
                    # Ordenar de mayor a menor para no alterar los índices de las filas restantes al borrar
                    indices = sorted(filas_para_eliminar['gsheet_id'].tolist(), reverse=True)
                    for idx in indices:
                        sheet.delete_rows(idx)
                    st.success("✅ Depuración de datos completada satisfactoriamente.")
                    time.sleep(1)
                    st.rerun()

            # Lógica para EDITAR (CON RECÁLCULO DE TIEMPO INTELIGENTE AL SINCRONIZAR)
            # Comparamos el dataframe original mostrado (df_display) con el editado
            original_data = df_display.drop(columns=['Fecha_Convertida', 'Mes_Nombre', 'Seleccionar'])
            edited_data = edited_df.drop(columns=['Seleccionar'])
            
            if not original_data.equals(edited_data):
                if st.button("💾 Sincronizar Ediciones Operativas"):
                    # Buscamos qué filas cambiaron comparando por gsheet_id
                    for i in range(len(original_data)):
                        if not original_data.iloc[i].equals(edited_data.iloc[i]):
                            fila = edited_data.iloc[i].copy()
                            row_idx = int(fila['gsheet_id'])
                            
                            # Lógica de Recálculo de Tiempo Automático Profesional al sincronizar
                            f_i_s = str(fila['Fecha_Inicio'])
                            h_i_s = str(fila['Hora_Inicio'])
                            f_f_s = str(fila['Fecha_Fin'])
                            h_f_s = str(fila['Hora_Fin'])
                            conoce_s = str(fila['Conocimiento_Tiempos'])
                            
                            duracion_recalculada = 0
                            
                            try:
                                if conoce_s == "Total" and f_f_s != "N/A" and h_f_s != "N/A":
                                    # Intentar parsear las fechas/horas editadas
                                    dt_ini = datetime.strptime(f"{f_i_s} {h_i_s}", "%d/%m/%Y %H:%M:%S")
                                    dt_fin = datetime.strptime(f"{f_f_s} {h_f_s}", "%d/%m/%Y %H:%M:%S")
                                    duracion_recalculada = round((dt_fin - dt_ini).total_seconds() / 3600, 2)
                                    if duracion_recalculada < 0: duracion_recalculada = 0
                                else:
                                    # Si el conocimiento es parcial o falta dato, la duración es 0
                                    duracion_recalculada = 0
                            except:
                                # En caso de error de parseo (formato incorrecto), Duration=0
                                duracion_recalculada = 0

                            # Actualizar Duracion_Horas en la fila editada
                            fila['Duracion_Horas'] = duracion_recalculada

                            # Preparación de datos final para GSheets (asegurando tipos nativos Python)
                            # Orden de columnas asumido en GSheets: Zona, Servicio, Categoria, Equipo, Fecha_I, Hora_I, Fecha_F, Hora_F, Clientes_Af, Causa, Desc, Duracion_H, Conocimiento_T
                            row_values = [str(x) if not isinstance(x, (int, float)) else x for x in fila.drop('gsheet_id').tolist()]
                            sheet.update(f"A{row_idx}:M{row_idx}", [row_values])
                    
                    st.success("✅ Sincronización completa. Los tiempos de duración han sido recalculados automáticamente.")
                    time.sleep(1)
                    st.rerun()
            
            # Exportación de Datos
            csv_m = df_mes.drop(columns=['gsheet_id', 'Fecha_Convertida', 'Mes_Nombre']).to_csv(index=False).encode('utf-8')
            st.download_button(label=f"📥 Exportar Reporte {mes_seleccionado} (CSV)", data=csv_m, 
                               file_name=f"Reporte_NOC_Multinet_{mes_seleccionado}.csv", mime='text/csv')

        else:
            st.info(f"No se detectan registros operativos para el ciclo de {mes_seleccionado}.")

        # --- ARCHIVO HISTÓRICO ---
        st.divider()
        st.header("📂 Repositorio Consolidado Histórico")
        st.caption("ℹ️ Acceda a los registros detallados de ciclos de análisis anteriores.")
        
        meses_con_datos = [m for m in meses_nombres if m in df_total['Mes_Nombre'].unique()]
        otros_meses = [m for m in meses_con_datos if m != mes_seleccionado]
        
        if otros_meses:
            for mes in otros_meses:
                with st.expander(f"📁 Consolidado Mensual: {mes}"):
                    # Mostrar tabla solo lectura profesional
                    df_hist = df_total[df_total['Mes_Nombre'] == mes].drop(columns=['gsheet_id', 'Fecha_Convertida', 'Mes_Nombre'])
                    st.dataframe(df_hist, use_container_width=True, hide_index=True)
                    
                    # Descarga específica por mes
                    csv_h = df_hist.to_csv(index=False).encode('utf-8')
                    st.download_button(label=f"Exportar Consolidado {mes} (CSV)", data=csv_h, 
                                       file_name=f"Historico_NOC_{mes}.csv", mime='text/csv', key=f"btn_{mes}")
        else:
            st.info("No se dispone de registros históricos en otros periodos.")

    else:
        st.info("La base de datos operativa se encuentra vacía.")
except Exception as e:
    st.error(f"Error Crítico en el Procesamiento de Datos Operativos: {e}")
