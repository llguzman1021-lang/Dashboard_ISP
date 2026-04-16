import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
from datetime import datetime
import calendar
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
        return client.open("Dashboard_ISP").sheet1
    except Exception as e:
        st.error(f"Error de enlace con la base de datos central: {e}")
        return None

sheet = conectar()
if sheet is None: st.stop()


# --- MOTOR DE CÁLCULOS KPI's ESTRATÉGICOS ---
def calcular_metricas(df_kpi, horas_mes_total):
    if df_kpi.empty:
        return 0.0, 0.0, 100.0, 0.0, 0, 0.0
    
    # Bruto Suma
    downtime_bruto = df_kpi['Duracion_Horas'].sum()
    
    # Promedio Afectación (ACD)
    mask_acd = (df_kpi['Duracion_Horas'] > 0) & (df_kpi['Clientes_Afectados'] > 0)
    acd = (df_kpi.loc[mask_acd, 'Duracion_Horas'] * df_kpi.loc[mask_acd, 'Clientes_Afectados']).sum() / df_kpi.loc[mask_acd, 'Clientes_Afectados'].sum() if mask_acd.any() else 0.0
    
    # SLA Cronológico Auténtico
    v_kpi = df_kpi[df_kpi['Conocimiento_Tiempos'] == 'Total'].copy()
    tiempo_real = 0.0
    if not v_kpi.empty:
        s = pd.to_datetime(v_kpi['Fecha_Inicio'] + ' ' + v_kpi['Hora_Inicio'], format="%d/%m/%Y %H:%M:%S", errors='coerce')
        e = pd.to_datetime(v_kpi['Fecha_Fin'] + ' ' + v_kpi['Hora_Fin'], format="%d/%m/%Y %H:%M:%S", errors='coerce')
        m = s.notna() & e.notna() & (s <= e)
        if m.any():
            i = sorted(list(zip(s[m].tolist(), e[m].tolist())), key=lambda x: x[0])
            mg = [list(i[0])]
            for c in i[1:]:
                if c[0] <= mg[-1][1]: 
                    mg[-1][1] = max(mg[-1][1], c[1])
                else: 
                    mg.append(list(c))
            tiempo_real = sum((end - stp).total_seconds() for stp, end in mg) / 3600.0
            
    sla_resultante = max(0.0, min(100.0, ((horas_mes_total - tiempo_real) / horas_mes_total) * 100))
    mttr = df_kpi[df_kpi['Duracion_Horas'] > 0]['Duracion_Horas'].mean() if not df_kpi[df_kpi['Duracion_Horas'] > 0].empty else 0.0
    clientes = int(df_kpi['Clientes_Afectados'].sum())
    max_downt = df_kpi['Duracion_Horas'].max() if not df_kpi.empty else 0.0
    
    return downtime_bruto, acd, sla_resultante, mttr, clientes, max_downt


# --- ESTILOS PROFESIONALES ---
st.markdown("""
    <style>
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
    [data-testid="stMetricLabel"] { 
        color: #808495 !important; 
        font-size: 15px !important; 
        font-weight: 500 !important; 
    }
    [data-testid="stMetricValue"] { 
        color: #ffffff !important; 
        font-size: 32px !important;
        font-weight: 700 !important;
    }
    .stAlert, .stMarkdown { border-radius: 8px; }
    </style>
    """, unsafe_allow_html=True)

# --- SIDEBAR: GESTIÓN OPERATIVA ---
with st.sidebar:
    st.title("🛡️ Centro de Operaciones")
    st.caption("Panel de Control Multinet | v3.0 NOC")
    
    meses_nombres = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                     "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    
    mes_actual_num = datetime.now().month
    mes_seleccionado = st.selectbox("📅 Ciclo de Análisis Mensual", meses_nombres, index=mes_actual_num-1)
    
    st.divider()
    st.header("📋 Reporte de Incidencias NOC")
    
    zona = st.text_input("📍 Localización del Nodo/Zona")
    
    c_serv1, c_serv2 = st.columns([2, 3])
    servicio = c_serv1.selectbox("📡 Servicio Afectado", ["Internet", "Cable TV (CATV)", "IPTV (Mnet+)"])
    categoria = c_serv2.selectbox("👥 Segmentación de Impacto", ["Red Multinet (Troncal)", "Cliente Corporativo"])
    
    equipo = st.selectbox("⚙️ Equipamiento Afectado", ["OLT", "RB/Mikrotik", "Switch", "ONU", "Servidor", "Fibra Principal", "Caja NAP"])
    
    st.write("---")
    st.write("⏱️ **Ventana Temporal de Inicio**")
    
    c1, c2 = st.columns(2)
    f_i = c1.date_input("🗓️ Fecha de Inicio")
    conoce_h_i = c1.radio("🕒 ¿Conoce Hora Inicio?", ["No", "Sí"], horizontal=True)
    
    if conoce_h_i == "Sí":
        h_i = c2.time_input("🕒 Hora de Apertura")
        hora_inicio_final = h_i.strftime("%H:%M:%S")
    else:
        hora_inicio_final = "N/A"

    st.info("ℹ️ Tenga en cuenta: Si la hora no es especificada, el sistema registrará 'N/A' y la duración será 0h.")

    st.write("---")
    st.write("📉 **Estado de Cierre (Cálculo de Tiempos)**")
    
    c_c1, c_c2 = st.columns(2)
    f_f = c_c1.date_input("🗓️ Fecha de Cierre")
    conoce_h_f = c_c1.radio("🕒 ¿Conoce Hora Cierre?", ["No", "Sí"], horizontal=True)
    
    if conoce_h_f == "Sí":
        h_f = c_c2.time_input("🕒 Hora de Cierre")
        final_h = h_f.strftime("%H:%M:%S")
    else:
        final_h = "N/A"

    st.info("ℹ️ Tenga en cuenta: Si la hora no es especificada, el sistema registrará 'N/A' y la duración será 0h.")

    duracion = 0
    if conoce_h_i == "Sí" and conoce_h_f == "Sí":
        desc_conocimiento = "Total"
        try:
            dt_i = datetime.combine(f_i, h_i)
            dt_f = datetime.combine(f_f, h_f)
            duracion = round((dt_f - dt_i).total_seconds() / 3600, 2)
            if duracion < 0:
                st.error("Error: La fecha/hora de cierre no puede ser anterior a la de inicio.")
                duracion = 0
        except:
            duracion = 0
    elif conoce_h_i == "No" and conoce_h_f == "No":
        desc_conocimiento = "Parcial (Solo Fechas)"
    elif conoce_h_i == "Sí" and conoce_h_f == "No":
        desc_conocimiento = "Parcial (Falta Hora Cierre)"
    else:
        desc_conocimiento = "Parcial (Falta Hora Inicio)"

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
    
    if st.button("Guardar Registro Operativo"):
        nueva_fila = [
            zona, servicio, categoria, equipo, f_i.strftime("%d/%m/%Y"), hora_inicio_final, 
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

        df_mes = df_total[df_total['Mes_Nombre'] == mes_seleccionado].copy()

        st.title(f"📊 Dashboard Operacional NOC: {mes_seleccionado} {datetime.now().year}")

        if not df_mes.empty:
            
            # --- MOTOR DE FILTROS GLOBALES ---
            with st.expander("🔎 Matriz de Segmentación de Variables", expanded=False):
                col_f1, col_f2, col_f3 = st.columns(3)
                
                f_zonas = col_f1.multiselect("Filtrar Zona Geográfica", options=df_mes['Zona'].unique().tolist())
                f_cats = col_f2.multiselect("Segmento Comercial", options=df_mes['Categoria'].unique().tolist())
                f_eqs = col_f3.multiselect("Equipamiento Afectado", options=df_mes['Equipo'].unique().tolist())
            
            df_filtrado = df_mes.copy()
            if f_zonas: df_filtrado = df_filtrado[df_filtrado['Zona'].isin(f_zonas)]
            if f_cats: df_filtrado = df_filtrado[df_filtrado['Categoria'].isin(f_cats)]
            if f_eqs: df_filtrado = df_filtrado[df_filtrado['Equipo'].isin(f_eqs)]

            if df_filtrado.empty:
                st.warning("⚠️ No existen registros con la combinación de filtros analíticos actual.")
            else:
                mes_index = meses_nombres.index(mes_seleccionado) + 1
                anio_actual = datetime.now().year
                dias_mes = calendar.monthrange(anio_actual, mes_index)[1]
                horas_totales_mes = dias_mes * 24

                # Extraer métricas actuales
                downtime_total, acd_horas, sla_porcentaje, avg_mttr, cl_imp, max_h = calcular_metricas(df_filtrado, horas_totales_mes)

                # --- COMPARATIVAS CON MES ANTERIOR (DELTAS) ---
                delta_m, delta_a, delta_s = None, None, None
                
                if mes_index > 1:
                    mes_pasado_nom = meses_nombres[mes_index - 2]
                    dias_pasado = calendar.monthrange(anio_actual, mes_index - 1)[1]
                    
                    df_pasado = df_total[df_total['Mes_Nombre'] == mes_pasado_nom].copy()
                    if f_zonas: df_pasado = df_pasado[df_pasado['Zona'].isin(f_zonas)]
                    if f_cats: df_pasado = df_pasado[df_pasado['Categoria'].isin(f_cats)]
                    if f_eqs: df_pasado = df_pasado[df_pasado['Equipo'].isin(f_eqs)]
                    
                    if not df_pasado.empty:
                        _, acd_p, sla_p, mttr_p, _, _ = calcular_metricas(df_pasado, dias_pasado * 24)
                        
                        if mttr_p > 0: delta_m = f"{avg_mttr - mttr_p:+.1f}h"
                        if acd_p > 0: delta_a = f"{acd_horas - acd_p:+.1f}h"
                        if sla_p > 0: delta_s = f"{sla_porcentaje - sla_p:+.2f}%"

                # Lógicas de render de Colores
                if sla_porcentaje >= 99.9: delta_c_s, s_val = "normal", "✅ Aprobado"
                elif sla_porcentaje >= 99.0: delta_c_s, s_val = "off", "⚠️ Margen Riesgo"
                else: delta_c_s, s_val = "inverse", "🚨 Alerta SLA"
                
                if acd_horas <= 2.0: delta_c_a, acd_val = "inverse", "✅ Excelente"
                elif acd_horas <= 6.0: delta_c_a, acd_val = "off", "⚠️ Moderado"
                else: delta_c_a, acd_val = "normal", "🚨 Crítico"

                # Tablero de Control Directivo Rediseñado
                st.write("---")
                k_mttr, k_imp, k_max, k_down, k_sla = st.columns(5)
                
                k_mttr.metric("⏱️ MTTR (Promedio)", f"{avg_mttr:.2f} hrs", delta=delta_m, delta_color="inverse")
                k_imp.metric("👥 Impacto Acumulado", f"{cl_imp} usuarios", help="Afectados en este periodo analítico.")
                k_max.metric("🚨 Máx Indisponibilidad", f"{max_h:.2f} hrs", help="Incidente aislado de peor desempeño.")
                k_down.metric("📶 Disponibilidad SLA", f"{sla_porcentaje:.2f}%", delta=delta_s, delta_color="normal", help=f"True Network Uptime Cronológico (Sin solapamientos). Estado Actual: {s_val}")
                k_sla.metric("📊 Promedio Afectación", f"{acd_horas:.2f} h/cli", delta=delta_a, delta_color="inverse", help=f"Average Customer Downtime (ACD). Estado Actual: {acd_val}")

                # --- VISUALIZACIÓN MULTI-FACTOR (LAS NUEVAS GRÁFICAS KPI) ---
                st.divider()
                st.subheader(f"📈 Inteligencia de Rendimiento: {mes_seleccionado}")
                
                # Fila 1: Salud Hardware y Causa Raíz
                col_g1, col_g2 = st.columns(2)
                
                df_req = df_filtrado.groupby('Equipo').size().reset_index(name='Fallos').sort_values('Fallos', ascending=True)
                fig_eq = px.bar(df_req, x='Fallos', y='Equipo', orientation='h', color='Fallos',
                                title="⚙️ <b>Hardware Health & Reliability</b>", template="plotly_dark", color_continuous_scale="Reds")
                fig_eq.update_layout(coloraxis_showscale=False, margin=dict(l=0, r=0, t=60, b=0))
                col_g1.plotly_chart(fig_eq, use_container_width=True)

                df_caus = df_filtrado.groupby('Causa').size().reset_index(name='Alertas')
                fig_rca = px.pie(df_caus, names='Causa', values='Alertas', hole=0.5,
                                title="🔍 <b>Análisis de Causa Raíz (RCA)</b>", template="plotly_dark")
                fig_rca.update_traces(textposition='inside', textinfo='percent+label', marker=dict(line=dict(color='#000000', width=1)))
                fig_rca.update_layout(showlegend=False, margin=dict(l=0, r=0, t=60, b=0))
                col_g2.plotly_chart(fig_rca, use_container_width=True)
                
                # Fila 2: Diagrama de Dispersión / Matriz de Riesgo Geográfico
                df_riesgo = df_filtrado.groupby('Zona').agg(Frecuencia=('gsheet_id', 'count'), Horas_Down=('Duracion_Horas', 'sum'), Afect_Totales=('Clientes_Afectados', 'sum')).reset_index()
                if not df_riesgo.empty and df_riesgo['Afect_Totales'].sum() > 0:
                    fig_sc = px.scatter(df_riesgo, x='Frecuencia', y='Horas_Down', size='Afect_Totales', color='Zona',
                                        title="📍 <b>Risk Matrix: Criticidad e Impacto por Sector Geográfico</b><br><sup>Tamaño burbuja: Volumen de clientes dañados</sup>",
                                        labels={'Frecuencia':'Eventos de Caída Totales', 'Horas_Down': 'Horas de Suma Downtime Apagón'},
                                        template="plotly_dark")
                    fig_sc.update_layout(margin=dict(l=0, r=0, t=70, b=0), showlegend=True)
                    st.plotly_chart(fig_sc, use_container_width=True)

                # --- VISUALIZACIONES CLÁSICAS CUIDADAS ---
                st.divider()
                
                df_trend = df_filtrado.groupby('Fecha_Convertida').size().reset_index(name='Total_Eventos')
                fig_trend = px.area(df_trend, x='Fecha_Convertida', y='Total_Eventos', 
                                    title="📊 <b>Fluctuación Cíclica (Día a Día) Analítico</b>",
                                    labels={'Fecha_Convertida': 'Fecha del Evento', 'Total_Eventos': 'Volumen Eventos'},
                                    template="plotly_dark")
                fig_trend.update_traces(line_color='#0068c9', fillcolor='rgba(0, 104, 201, 0.2)')
                st.plotly_chart(fig_trend, use_container_width=True)

                # --- BITÁCORA INTELIGENTE PROTEGIDA ---
                st.divider()
                st.subheader(f"🔍 Auditoría Aislada de Base de Datos")
                
                col_a1, col_a2 = st.columns([3,2])
                busqueda = col_a1.text_input("🔎 Search Logs:", placeholder="Zonas, hardware, reportes...")
                pin_seguridad = col_a2.text_input("🔐 Passcode Admin:", type="password", placeholder="PIN Maestro para Editar/Borrar")
                
                acceso_autorizado = (pin_seguridad == "1010")

                df_display = df_filtrado.copy()

                if busqueda:
                    mask = df_display.astype(str).apply(lambda x: x.str.contains(busqueda, case=False, na=False)).any(axis=1)
                    df_display = df_display[mask]
                
                df_display.insert(0, "Seleccionar", False)
                
                conoce_opciones = ["Total", "Parcial (Solo Fechas)", "Parcial (Falta Hora Cierre)", "Parcial (Falta Hora Inicio)", "Parcial (Solo Fecha)", "Parcial (Solo Hora)", "Ninguno"]
                servicio_opciones = ["Internet", "Cable TV (CATV)", "IPTV (Mnet+)"]

                edited_df = st.data_editor(
                    df_display.drop(columns=['Fecha_Convertida', 'Mes_Nombre']),
                    column_config={
                        "Seleccionar": st.column_config.CheckboxColumn("Sel", default=False), 
                        "gsheet_id": None,
                        "Servicio": st.column_config.SelectboxColumn("Servicio", options=servicio_opciones, required=True),
                        "Fecha_Inicio": st.column_config.TextColumn("F. Inicio"),
                        "Hora_Inicio": st.column_config.TextColumn("H. Inicio"),
                        "Fecha_Fin": st.column_config.TextColumn("F. Cierre"),
                        "Hora_Fin": st.column_config.TextColumn("H. Cierre"),
                        "Duracion_Horas": st.column_config.NumberColumn("Duración", disabled=True, format="%.2f"),
                        "Conocimiento_Tiempos": st.column_config.SelectboxColumn("Tipo Conocimiento", options=conoce_opciones, required=True)
                    },
                    use_container_width=True, hide_index=True, num_rows="fixed", key="main_editor"
                )

                # --- LÓGICA DE AUDITORÍA REQUERIMIENTO PIN ---
                filas_para_eliminar = edited_df[edited_df["Seleccionar"] == True]
                original_data = df_display.drop(columns=['Fecha_Convertida', 'Mes_Nombre', 'Seleccionar'])
                edited_data = edited_df.drop(columns=['Seleccionar'])
                hay_cambios = not original_data.equals(edited_data)

                if not filas_para_eliminar.empty or hay_cambios:
                    if acceso_autorizado:
                        if not filas_para_eliminar.empty:
                            if st.button(f"🗑️ Confirmar Ejecutar Borrado de ({len(filas_para_eliminar)}) filas en Nube"):
                                indices = sorted(filas_para_eliminar['gsheet_id'].tolist(), reverse=True)
                                for idx in indices: sheet.delete_rows(idx)
                                st.success("✅ Filas destruidas. Base resincronizada.")
                                time.sleep(1)
                                st.rerun()

                        if hay_cambios:
                            if st.button("💾 Inyectar Cambios y Forzar SLA a Nube"):
                                for i in range(len(original_data)):
                                    if not original_data.iloc[i].equals(edited_data.iloc[i]):
                                        fila = edited_data.iloc[i].copy()
                                        row_idx = int(fila['gsheet_id'])
                                        
                                        # Recalcular interno solo editado (código simplificado resguarda gsheets)
                                        f_i_s, h_i_s = str(fila['Fecha_Inicio']), str(fila['Hora_Inicio'])
                                        f_f_s, h_f_s = str(fila['Fecha_Fin']), str(fila['Hora_Fin'])
                                        conoce_s = str(fila['Conocimiento_Tiempos'])
                                        
                                        dur_r = 0
                                        try:
                                            if conoce_s == "Total" and f_f_s != "N/A" and h_f_s != "N/A" and h_i_s != "N/A":
                                                dt_ini = datetime.strptime(f"{f_i_s} {h_i_s}", "%d/%m/%Y %H:%M:%S")
                                                dt_fin = datetime.strptime(f"{f_f_s} {h_f_s}", "%d/%m/%Y %H:%M:%S")
                                                dur_r = round((dt_fin - dt_ini).total_seconds() / 3600, 2)
                                                if dur_r < 0: dur_r = 0
                                        except:
                                            dur_r = 0

                                        fila['Duracion_Horas'] = dur_r
                                        row_values = [str(x) if not isinstance(x, (int, float)) else x for x in fila.drop('gsheet_id').tolist()]
                                        sheet.update(f"A{row_idx}:M{row_idx}", [row_values])
                                
                                st.success("✅ Archivo Operacional Salvado e Inyectado.")
                                time.sleep(1)
                                st.rerun()
                    else:
                        st.error("🛑 Módulo Directivo Protegido. Inserte el Passcode ('1010') del panel contiguo para confirmar eliminación y alterado de datos.")
                
                # Exportación
                st.write("---")
                csv_m = df_filtrado.drop(columns=['gsheet_id', 'Fecha_Convertida', 'Mes_Nombre']).to_csv(index=False).encode('utf-8')
                st.download_button(f"📥 Exportar Reporte En Pantalla {mes_seleccionado} (CSV)", data=csv_m, file_name=f"Reporte_NOC_{mes_seleccionado}.csv", mime='text/csv')

        else:
            st.info(f"Ausencia de registros para {mes_seleccionado}.")

        # --- ARCHIVO HISTÓRICO ---
        st.divider()
        st.header("📂 Repositorio Consolidado Histórico")
        
        meses_con_datos = [m for m in meses_nombres if m in df_total['Mes_Nombre'].unique()]
        otros_meses = [m for m in meses_con_datos if m != mes_seleccionado]
        
        if otros_meses:
            for mes in otros_meses:
                with st.expander(f"📁 Database Mensual Cruda: {mes}"):
                    df_hist = df_total[df_total['Mes_Nombre'] == mes].drop(columns=['gsheet_id', 'Fecha_Convertida', 'Mes_Nombre'])
                    st.dataframe(df_hist, use_container_width=True, hide_index=True)
                    csv_h = df_hist.to_csv(index=False).encode('utf-8')
                    st.download_button(label=f"Exportar Historico {mes} (CSV)", data=csv_h, file_name=f"Historico_NOC_{mes}.csv", mime='text/csv', key=f"btn_{mes}")

except Exception as e:
    st.error(f"Error Técnico Consolidado: {e}")
