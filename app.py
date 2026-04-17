import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
from datetime import datetime
import calendar
import re
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
        return client.open("Dashboard_ISP")
    except Exception as e:
        st.error(f"Error de enlace con la base de datos central: {e}")
        return None

spreadsheet = conectar()
if spreadsheet is None: st.stop()
sheet = spreadsheet.sheet1


# --- MOTOR DE CÁLCULOS KPI's ESTRATÉGICOS ---
def calcular_metricas(df_kpi, horas_mes_total):
    if df_kpi.empty:
        return 0.0, 0.0, 100.0, 0.0, 0, 0.0
    
    # Bruto Suma Horas
    downtime_bruto = df_kpi['Duracion_Horas'].sum()
    
    # Promedio Afectación (ACD)
    mask_acd = (df_kpi['Duracion_Horas'] > 0) & (df_kpi['Clientes_Afectados'] > 0)
    acd = (df_kpi.loc[mask_acd, 'Duracion_Horas'] * df_kpi.loc[mask_acd, 'Clientes_Afectados']).sum() / df_kpi.loc[mask_acd, 'Clientes_Afectados'].sum() if mask_acd.any() else 0.0
    
    # Disponibilidad Porcentual (% Uptime)
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
    st.title("🏢 Centro de Operaciones de Red (NOC)")
    st.caption("Panel de Control Gerencial Multinet | v3.3")
    
    meses_nombres = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                     "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    
    mes_actual_num = datetime.now().month
    mes_seleccionado = st.selectbox("📅 Ciclo de Análisis Mensual", meses_nombres, index=mes_actual_num-1)
    
    st.divider()
    st.header("📋 Formulario de Incidencias Operativas")
    
    zona = st.text_input("📍 Ubicación de la Incidencia (Nodo o Zona)")
    
    c_serv1, c_serv2 = st.columns([2, 3])
    servicio = c_serv1.selectbox("🌐 Servicio Principal Afectado", ["Internet", "Cable TV (CATV)", "IPTV (Mnet+)"])
    categoria = c_serv2.selectbox("🏢 Segmento de Mercado", ["Red Multinet (Troncal)", "Cliente Corporativo"])
    
    equipo = st.selectbox("🖥️ Equipamiento de Red Afectado", ["OLT", "RB/Mikrotik", "Switch", "ONU", "Servidor", "Fibra Principal", "Caja NAP", "Mufa", "Splitter", "Sistema UNIFI", "Antenas Ubiquiti"])
    
    st.write("---")
    st.write("⏱️ **Registro de Tiempos: Inicio de Falla**")
    
    c1, c2 = st.columns(2)
    f_i = c1.date_input("📅 Fecha de Inicio")
    conoce_h_i = c1.radio("🕒 ¿Conoce la Hora Exacta?", ["No", "Sí"], horizontal=True, key="conoce_hi")
    
    if conoce_h_i == "Sí":
        h_i = c2.time_input("🕒 Hora de Apertura")
        hora_inicio_final = h_i.strftime("%H:%M:%S")
    else:
        hora_inicio_final = "N/A"

    st.info("ℹ️ Si omite la hora, se considerará 'N/A' y la duración contable será 0 horas.")

    st.write("---")
    st.write("✅ **Registro de Tiempos: Resolución y Cierre**")
    
    c_c1, c_c2 = st.columns(2)
    f_f = c_c1.date_input("📅 Fecha de Cierre")
    conoce_h_f = c_c1.radio("🕒 ¿Conoce la Hora Exacta?", ["No", "Sí"], horizontal=True, key="conoce_hf")
    
    if conoce_h_f == "Sí":
        h_f = c_c2.time_input("🕒 Hora de Cierre")
        final_h = h_f.strftime("%H:%M:%S")
    else:
        final_h = "N/A"

    st.info("ℹ️ Si omite la hora, se considerará 'N/A' y la duración contable será 0 horas.")

    duracion = 0
    if conoce_h_i == "Sí" and conoce_h_f == "Sí":
        desc_conocimiento = "Total"
        try:
            dt_i = datetime.combine(f_i, h_i)
            dt_f = datetime.combine(f_f, h_f)
            duracion = round((dt_f - dt_i).total_seconds() / 3600, 2)
            if duracion < 0:
                st.error("⚠️ Error: La fecha y hora de cierre no pueden ser anteriores a las de inicio.")
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
    if categoria == "Cliente Corporativo":
        clientes = 1
        st.info("🏢 Segmento Corporativo seleccionado: El sistema contabiliza automáticamente el impacto como 1 enlace (1 cliente corporativo afectado).")
    else:
        clientes = st.number_input("👤 Cantidad de Clientes Afectados (Unidades)", min_value=0, step=1)

    causa = st.selectbox("🛠️ Diagnóstico Técnico (Causa Raíz)", [
        "Corte de Fibra Óptica por Terceros (Vandalismo/Accidente)",
        "Caída de Árboles o Ramas sobre Tendido de Fibra",
        "Interrupción de Energía Comercial (Falla Eléctrica/Tormenta)",
        "Corrosión Salina en Equipos o Nodos (Zonas Costeras)",
        "Daños por Fauna Silvestre o Aves (Zonas Boscosas/Rurales)",
        "Falla de Hardware (Desgaste, Daño o Sobrecalentamiento)",
        "Desajuste de Configuración Lógica (Software/Routing)",
        "Falla de Redundancia en Anillo de Fibra",
        "Saturación de Tráfico o Cuello de Botella en la Red",
        "Saturación de Disco en Servidor UNIFI",
        "Problemas de Inicio en Servidor UNIFI",
        "Mantenimiento Programado o Ventana de Trabajo",
        "Vandalismo, Hurto o Sabotaje Directo",
        "Condiciones Climáticas Adversas (Tormentas, Fuertes Vientos)"
    ])
    desc = st.text_area("📝 Descripción Técnica y Detallada del Incidente")
    
    if st.button("💾 Guardar Registro Operativo"):
        meses_nombres_es = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
        nombre_pestana = f"{meses_nombres_es[f_i.month - 1]} {f_i.year}"
        
        try:
            target_sheet = spreadsheet.worksheet(nombre_pestana)
        except gspread.exceptions.WorksheetNotFound:
            target_sheet = spreadsheet.add_worksheet(title=nombre_pestana, rows="1000", cols="20")
            encabezados = ["Zona", "Servicio", "Categoria", "Equipo_Afectado", "Fecha_Inicio", "Hora_Inicio", "Fecha_Fin", "Hora_Fin", "Clientes_Afectados", "Causa_Raiz", "Descripcion", "Duracion_Horas", "Conocimiento_Tiempos"]
            target_sheet.append_row(encabezados)

        nueva_fila = [
            zona, servicio, categoria, equipo, f_i.strftime("%d/%m/%Y"), hora_inicio_final, 
            f_f.strftime("%d/%m/%Y"), final_h, int(clientes), causa, desc, duracion, desc_conocimiento
        ]
        target_sheet.append_row(nueva_fila)
        st.toast(f"✅ Información almacenada exitosamente en el periodo '{nombre_pestana}'.")
        time.sleep(1)
        st.rerun()

# --- PROCESAMIENTO Y ANALÍTICA DE DATOS ---
try:
    all_records = []
    
    # Helper to parse Spanish dates
    def parse_fecha(fecha_str):
        if not isinstance(fecha_str, str): return "N/A"
        fecha_str = fecha_str.strip().lower()
        if not fecha_str or fecha_str == "n/a": return "N/A"
        meses_dict = {"enero": "01", "febrero": "02", "marzo": "03", "abril": "04", "mayo": "05", "junio": "06", 
                      "julio": "07", "agosto": "08", "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"}
        match = re.search(r"(\d{1,2})\s+de\s+([a-z]+)", fecha_str)
        if match:
            dia = match.group(1).zfill(2)
            mes = meses_dict.get(match.group(2), "01")
            return f"{dia}/{mes}/2026"
        return fecha_str
        
    def parse_hora(hora_str):
        if not isinstance(hora_str, str): return "N/A"
        hora_str = hora_str.strip().upper()
        if not hora_str or hora_str == "N/A": return "N/A"
        match = re.search(r"(\d{1,2})[.:](\d{2})\s*(AM|PM)?", hora_str)
        if match:
            h, m, ampm = match.groups()
            h = int(h)
            if ampm == "PM" and h < 12: h += 12
            if ampm == "AM" and h == 12: h = 0
            return f"{str(h).zfill(2)}:{m}:00"
        return "N/A"

    for ws in spreadsheet.worksheets():
        ws_records = ws.get_all_records()
        if not ws_records: continue
        
        keys = list(ws_records[0].keys())
        is_old = any(k.upper() == 'ZONA' for k in keys) and not any(k == 'Causa_Raiz' for k in keys)
        
        row_idx = 2
        for r in ws_records:
            if is_old:
                f_ini = parse_fecha(r.get('FECHA DE INICIO', ''))
                f_fin = parse_fecha(r.get('FECHA DE FINALIZACIÓN', ''))
                h_ini = parse_hora(r.get('HORA DE INICIO', ''))
                h_fin = parse_hora(r.get('HORA DE FINALIZACIÓN', ''))
                
                dur = 0.0
                if f_ini != "N/A" and f_fin != "N/A" and h_ini != "N/A" and h_fin != "N/A":
                    try:
                        dt_i = datetime.strptime(f"{f_ini} {h_ini}", "%d/%m/%Y %H:%M:%S")
                        dt_f = datetime.strptime(f"{f_fin} {h_fin}", "%d/%m/%Y %H:%M:%S")
                        dur = round((dt_f - dt_i).total_seconds() / 3600, 2)
                        if dur < 0: dur = 0.0
                    except:
                        pass
                
                conocimiento = "Total" if (h_ini != "N/A" and h_fin != "N/A") else "Parcial (Solo Fechas)"
                
                desc = str(r.get('DESCRIPCIÓN DE LA FALLA', ''))
                match_clientes = re.search(r'(\d+)\s+clientes', desc, re.IGNORECASE)
                cl = int(match_clientes.group(1)) if match_clientes else 0

                normalized_r = {
                    "Zona": r.get('ZONA', 'Desconocido'),
                    "Servicio": "Internet",
                    "Categoria": "Histórico",
                    "Equipo_Afectado": r.get('EQUIPO AFECTADO', 'No Especificado'),
                    "Fecha_Inicio": f_ini,
                    "Hora_Inicio": h_ini,
                    "Fecha_Fin": f_fin,
                    "Hora_Fin": h_fin,
                    "Clientes_Afectados": cl,
                    "Causa_Raiz": "No Especificado",
                    "Descripcion": desc,
                    "Duracion_Horas": dur,
                    "Conocimiento_Tiempos": conocimiento
                }
                normalized_r['gsheet_id'] = row_idx
                normalized_r['worksheet_name'] = ws.title
                all_records.append(normalized_r)
            else:
                r['gsheet_id'] = row_idx
                r['worksheet_name'] = ws.title
                all_records.append(r)
            row_idx += 1

    if all_records:
        df_total = pd.DataFrame(all_records)
        
        # --- AUTO-DETECCIÓN INTELIGENTE DE ENCABEZADOS EN GDOCS ---
        cols_drive = df_total.columns.tolist()
        COL_EQUIPO = next((c for c in cols_drive if 'equip' in c.lower()), cols_drive[3] if len(cols_drive) > 3 else 'Equipo')
        COL_CAUSA = next((c for c in cols_drive if 'causa' in c.lower() or 'diag' in c.lower()), cols_drive[9] if len(cols_drive) > 9 else 'Causa')
        
        df_total['Fecha_Convertida'] = pd.to_datetime(df_total['Fecha_Inicio'], dayfirst=True, errors='coerce')
        df_total['Mes_Nombre'] = df_total['Fecha_Convertida'].dt.month.map(lambda x: meses_nombres[int(x)-1] if pd.notnull(x) else None)

        df_mes = df_total[df_total['Mes_Nombre'] == mes_seleccionado].copy()

        st.title(f"📊 Dashboard Operacional NOC: {mes_seleccionado} {datetime.now().year}")

        if not df_mes.empty:
            
            # --- MOTOR DE FILTROS GLOBALES ---
            with st.expander("🔎 Filtrar Datos Mostrados (Zonas, Equipos, Servicios...)", expanded=False):
                col_f1, col_f2, col_f3 = st.columns(3)
                
                zonas_fijas = ["El Rosario", "La Costa del Sol", "La Libertad", "Rio Mar", "El Tunco", "Zaragoza", "Zacatecoluca", "San Salvador", "San Miguel Tepezontes", "ARG", "Santiago Nonualco"]
                categorias_fijas = ["Red Multinet (Troncal)", "Cliente Corporativo"]
                equipos_fijos = ["OLT", "RB/Mikrotik", "Switch", "ONU", "Servidor", "Fibra Principal", "Caja NAP", "Mufa", "Splitter", "Sistema UNIFI", "Antenas Ubiquiti"]
                
                f_zonas = col_f1.multiselect("Filtrar Zona Geográfica", options=zonas_fijas)
                f_cats = col_f2.multiselect("Segmento Comercial", options=categorias_fijas)
                f_eqs = col_f3.multiselect("Equipamiento Afectado", options=equipos_fijos)
            
            df_filtrado = df_mes.copy()
            if f_zonas: df_filtrado = df_filtrado[df_filtrado['Zona'].isin(f_zonas)]
            if f_cats: df_filtrado = df_filtrado[df_filtrado['Categoria'].isin(f_cats)]
            if f_eqs: df_filtrado = df_filtrado[df_filtrado[COL_EQUIPO].isin(f_eqs)]

            if df_filtrado.empty:
                st.warning("⚠️ No existen fallas registradas que coincidan con los filtros seleccionados.")
            else:
                mes_index = meses_nombres.index(mes_seleccionado) + 1
                anio_actual = datetime.now().year
                dias_mes = calendar.monthrange(anio_actual, mes_index)[1]
                horas_totales_mes = dias_mes * 24

                # Extraer métricas actuales
                downtime_total, acd_horas, sla_porcentaje, avg_mttr, cl_imp, max_h = calcular_metricas(df_filtrado, horas_totales_mes)
                dias_totales = downtime_total / 24.0

                # --- COMPARATIVAS CON MES ANTERIOR (DELTAS) ---
                delta_m, delta_a, delta_s, delta_dias = None, None, None, None
                
                if mes_index > 1:
                    mes_pasado_nom = meses_nombres[mes_index - 2]
                    dias_pasado = calendar.monthrange(anio_actual, mes_index - 1)[1]
                    
                    df_pasado = df_total[df_total['Mes_Nombre'] == mes_pasado_nom].copy()
                    if f_zonas: df_pasado = df_pasado[df_pasado['Zona'].isin(f_zonas)]
                    if f_cats: df_pasado = df_pasado[df_pasado['Categoria'].isin(f_cats)]
                    if f_eqs: df_pasado = df_pasado[df_pasado[COL_EQUIPO].isin(f_eqs)]
                    
                    if not df_pasado.empty:
                        d_b_p, acd_p, sla_p, mttr_p, _, _ = calcular_metricas(df_pasado, dias_pasado * 24)
                        
                        if mttr_p > 0: delta_m = f"{avg_mttr - mttr_p:+.1f} horas"
                        if acd_p > 0: delta_a = f"{acd_horas - acd_p:+.1f} horas"
                        if sla_p > 0: delta_s = f"{sla_porcentaje - sla_p:+.2f}%"
                        if d_b_p > 0: 
                            dias_p = d_b_p / 24.0
                            delta_dias = f"{dias_totales - dias_p:+.1f} días"

                # Tablero de Control Directivo Rediseñado en 2 Celdas Expandidas
                st.write("---")
                # Fila 1
                k1, k2, k3 = st.columns(3)
                k1.metric("⏱️ Tiempo Promedio de Resolución (MTTR)", f"{avg_mttr:.2f} horas", delta=delta_m, delta_color="inverse", help="Promedio de horas empleadas para reparar el servicio tras la notificación de la falla.")
                k2.metric("👥 Total de Clientes Interrumpidos", f"{cl_imp} clientes", help="Cantidad consolidada de usuarios que experimentaron cortes de servicio. (Nota: Estimación en casos sin telemetría exacta).")
                k3.metric("⏳ Impacto Operativo Acumulado", f"{dias_totales:.2f} días", delta=delta_dias, delta_color="inverse", help="Sumatoria global del tiempo de desconexión expresado en la equivalencia de días enteros.")
                
                # Fila 2
                st.write("") # Pequeño separador visual 
                k4, k5, k6 = st.columns(3)
                k4.metric("🛑 Duración de la Falla Más Crítica", f"{max_h:.2f} horas", help="La duración en horas del incidente más severo y prolongado registrado en este periodo mensual.")
                k5.metric("📈 Porcentaje de Disponibilidad (SLA)", f"{sla_porcentaje:.2f}%", delta=delta_s, delta_color="normal", help="Nivel integral de servicio operativo (Service Level Agreement) basado en las horas del mes.")
                k6.metric("📉 Promedio de Afectación por Cliente (ACD)", f"{acd_horas:.2f} horas / cliente", delta=delta_a, delta_color="inverse", help="Promedio estadístico de horas continuas en las que un cliente experimentó interrupción de servicio (Afectación Promedio).")
                
                st.caption("ℹ️ **Nota sobre Clientes Afectados:** La cantidad de clientes mostrada es una estimación. Cuando no se cuenta con el dato exacto (como en fallas generales o registros antiguos), el sistema usa un valor base para no alterar los promedios.")

                # --- VISUALIZACIÓN MULTI-FACTOR (LAS NUEVAS GRÁFICAS KPI) ---
                st.divider()
                st.subheader(f"📈 Análisis Visual del Rendimiento Operativo")
                
                # Fila 1: Motivos y Hardware (Las nuevas)
                col_g1, col_g2 = st.columns(2)
                
                df_caus = df_filtrado.groupby(COL_CAUSA).size().reset_index(name='Alertas')
                fig_rca = px.pie(df_caus, names=COL_CAUSA, values='Alertas', hole=0.5,
                                title="🔍 <b>Causas Principales de las Fallas Registradas</b>", template="plotly_dark")
                fig_rca.update_traces(textposition='inside', textinfo='percent+label', marker=dict(line=dict(color='#000000', width=1)))
                fig_rca.update_layout(showlegend=False, margin=dict(l=0, r=0, t=60, b=0))
                col_g1.plotly_chart(fig_rca, use_container_width=True)

                df_req = df_filtrado.groupby(COL_EQUIPO).size().reset_index(name='Fallos').sort_values('Fallos', ascending=True)
                fig_eq = px.bar(df_req, x='Fallos', y=COL_EQUIPO, orientation='h', color='Fallos',
                                title="🛠️ <b>Fallas Acumuladas por Tipo de Equipamiento</b>", template="plotly_dark", color_continuous_scale="Reds")
                fig_eq.update_layout(coloraxis_showscale=False, margin=dict(l=0, r=0, t=60, b=0), xaxis_title="Cantidad de Eventos (Fallas)", yaxis_title="")
                col_g2.plotly_chart(fig_eq, use_container_width=True)

                # Fila 2: Servicios y Zonas Críticas (Las viejas restauradas)
                st.write("")
                col_g3, col_g4 = st.columns(2)
                
                df_serv = df_filtrado.groupby('Servicio').size().reset_index(name='Total_Eventos')
                fig_serv_kpi = px.bar(df_serv, x='Total_Eventos', y='Servicio', orientation='h',
                                    title="🌐 <b>Volumen de Incidencias según el Servicio</b>",
                                    text_auto=True, template="plotly_dark", color='Servicio', color_discrete_sequence=['#0068c9', '#ff9f43', '#27ae60'])
                fig_serv_kpi.update_layout(showlegend=False, margin=dict(l=0, r=0, t=60, b=0), xaxis_title="Total de Caídas Registradas", yaxis_title="")
                fig_serv_kpi.update_traces(textposition='inside', textfont_size=14, marker_line_width=0)
                col_g3.plotly_chart(fig_serv_kpi, use_container_width=True)
                
                top_zonas = df_filtrado.groupby('Zona')['Duracion_Horas'].sum().nlargest(5).reset_index()
                top_zonas.columns = ['Zona', 'Horas Offline']
                top_zonas['Etiqueta'] = top_zonas['Horas Offline'].apply(lambda x: f"{x:.2f} horas")
                fig_bar_zonas = px.bar(top_zonas, x='Horas Offline', y='Zona', orientation='h',
                                    title="📉 <b>Impacto por Zona (Horas sin Servicio)</b>",
                                    text='Etiqueta', template="plotly_dark", color='Horas Offline', color_continuous_scale='Blues')
                fig_bar_zonas.update_layout(coloraxis_showscale=False, yaxis={'categoryorder':'total ascending'}, margin=dict(l=0, r=0, t=60, b=0), xaxis_title="Total de Horas sin Servicio", yaxis_title="")
                fig_bar_zonas.update_traces(marker_line_width=0, textfont_size=13, textposition='inside')
                col_g4.plotly_chart(fig_bar_zonas, use_container_width=True)

                # Fila 3 y 4: Dispersión Macro y Tendencia Temporal
                st.write("---")
                df_riesgo = df_filtrado.groupby('Zona').agg(Frecuencia=('gsheet_id', 'count'), Horas_Down=('Duracion_Horas', 'sum'), Afect_Totales=('Clientes_Afectados', 'sum')).reset_index()
                if not df_riesgo.empty and df_riesgo['Afect_Totales'].sum() > 0:
                    fig_sc = px.scatter(df_riesgo, x='Frecuencia', y='Horas_Down', size='Afect_Totales', color='Zona',
                                        title="📍 <b>Matriz de Riesgo: Desempeño Crítico por Zonas de Cobertura</b><br><sup>Nodos en el eje superior presentan tiempos de resolución prolongados. Nodos hacia la derecha sufren fallas recurrentes. El radio del círculo representa el volumen de clientes afectados.</sup>",
                                        labels={'Frecuencia':'Cantidad de Fallas Registradas', 'Horas_Down': 'Horas Totales Caídas (Acumulado)'},
                                        template="plotly_dark")
                    fig_sc.update_layout(margin=dict(l=0, r=0, t=70, b=0), showlegend=True)
                    st.plotly_chart(fig_sc, use_container_width=True)

                st.write("")
                df_trend = df_filtrado.groupby('Fecha_Convertida').size().reset_index(name='Total_Eventos')
                fig_trend = px.area(df_trend, x='Fecha_Convertida', y='Total_Eventos', 
                                    title="📅 <b>Tendencia Diaria de Cortes Operativos</b><br><sup>Muestra la fluctuación y volumen de las incidencias registradas día a día durante este ciclo de facturación.</sup>",
                                    labels={'Fecha_Convertida': 'Fechas del Mes', 'Total_Eventos': 'Cantidad Total de Fallas'},
                                    template="plotly_dark")
                fig_trend.update_traces(line_color='#0068c9', fillcolor='rgba(0, 104, 201, 0.2)')
                st.plotly_chart(fig_trend, use_container_width=True)


                # --- BITÁCORA INTELIGENTE PROTEGIDA ---
                st.divider()
                st.subheader(f"🔐 Panel de Auditoría y Mantenimiento de Datos")
                
                col_a1, col_a2 = st.columns([3,2])
                busqueda = col_a1.text_input("🔎 Búsqueda de Registros:", placeholder="Escriba aquí para ubicar detalles técnicos o zonas geográficas...")
                pin_seguridad = col_a2.text_input("🔑 Ingreso de PIN Restringido:", type="password", placeholder="Credencial de Administrador (Necesario para Editar o Eliminar)")
                
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
                        "worksheet_name": None,
                        "Servicio": st.column_config.SelectboxColumn("Servicio", options=servicio_opciones, required=True),
                        "Fecha_Inicio": st.column_config.TextColumn("F. Inicio"),
                        "Hora_Inicio": st.column_config.TextColumn("H. Inicio"),
                        "Fecha_Fin": st.column_config.TextColumn("F. Cierre"),
                        "Hora_Fin": st.column_config.TextColumn("H. Cierre"),
                        "Duracion_Horas": st.column_config.NumberColumn("Duración (Horas)", disabled=True, format="%.2f"),
                        "Conocimiento_Tiempos": st.column_config.SelectboxColumn("Nivel de Precisión del Registro", options=conoce_opciones, required=True)
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
                            if st.button(f"🗑️ Eliminar Definitivamente ({len(filas_para_eliminar)}) Registros Seleccionados"):
                                for ws_name, group in filas_para_eliminar.groupby('worksheet_name'):
                                    ws_target = spreadsheet.worksheet(ws_name)
                                    indices = sorted(group['gsheet_id'].tolist(), reverse=True)
                                    for idx in indices: ws_target.delete_rows(idx)
                                st.success("✅ Los registros han sido destruidos de forma segura.")
                                time.sleep(1)
                                st.rerun()

                        if hay_cambios:
                            if st.button("💾 Guardar Modificaciones y Recalcular Métricas Automáticamente"):
                                for i in range(len(original_data)):
                                    if not original_data.iloc[i].equals(edited_data.iloc[i]):
                                        fila = edited_data.iloc[i].copy()
                                        row_idx = int(fila['gsheet_id'])
                                        ws_name = fila['worksheet_name']
                                        ws_target = spreadsheet.worksheet(ws_name)
                                        
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
                                        row_values = [str(x) if not isinstance(x, (int, float)) else x for x in fila.drop(['gsheet_id', 'worksheet_name']).tolist()]
                                        ws_target.update(f"A{row_idx}:M{row_idx}", [row_values])
                                
                                st.success("✅ Modificaciones integradas exitosamente en la base de datos principal.")
                                time.sleep(1)
                                st.rerun()
                    else:
                        st.error("🛑 Operación Restringida: Ingrese el PIN de credencial de administrador para validar estas acciones.")
                
                # --- Exportación Final ---
                st.write("---")
                csv_m = df_filtrado.drop(columns=['gsheet_id', 'worksheet_name', 'Fecha_Convertida', 'Mes_Nombre']).to_csv(index=False).encode('utf-8')
                st.download_button(f"📥 Exportar Análisis del Mes a formato Excel (CSV)", data=csv_m, file_name=f"Reporte_Directivo_NOC.csv", mime='text/csv')

        else:
            st.info(f"🟢 Excelente estado operativo: No se detectan fallas mayores registradas para el ciclo mensual de {mes_seleccionado}.")


        # --- ARCHIVO HISTÓRICO MASIVO ---
        st.divider()
        st.header("📂 Histórico Consolidado de Datos Operativos (Mensual)")
        st.markdown("A continuación se presenta un desglose de los datos históricos mes a mes registrados en el sistema. Despliegue cualquier sección para auditar incidentes pasados.")
        
        meses_con_datos = [m for m in meses_nombres if m in df_total['Mes_Nombre'].unique()]
        otros_meses = [m for m in meses_con_datos if m != mes_seleccionado]
        
        if otros_meses:
            for mes in otros_meses:
                with st.expander(f"📁 Consultar Registro Histórico Completo: {mes}"):
                    df_hist = df_total[df_total['Mes_Nombre'] == mes].drop(columns=['gsheet_id', 'worksheet_name', 'Fecha_Convertida', 'Mes_Nombre'])
                    st.dataframe(df_hist, use_container_width=True, hide_index=True)
                    csv_h = df_hist.to_csv(index=False).encode('utf-8')
                    st.download_button(label=f"📥 Descargar Resumen Excel ({mes})", data=csv_h, file_name=f"Auditoria_Historica_NOC_{mes}.csv", mime='text/csv', key=f"btn_{mes}")

except Exception as e:
    st.error(f"⚠️ Error Interno del Motor de Procesamiento de Datos: {e}")
