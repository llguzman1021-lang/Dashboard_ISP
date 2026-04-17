import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px
from datetime import datetime
import calendar
import time

# =====================================================================
# [ETIQUETA: CONFIGURACIÓN INICIAL Y ESTILOS AVANZADOS]
# =====================================================================
st.set_page_config(
    page_title="Multinet NOC Analytics | Enterprise Operations",
    layout="wide",
    page_icon="🌐"
)

st.markdown("""
    <style>
    /* Diseño de botones principales */
    div.stButton > button:first-child {
        background-color: #0068c9; color: white; border-radius: 8px; width: 100%; font-weight: 600; border: none; transition: all 0.2s;
    }
    div.stButton > button:first-child:hover { background-color: #0056a3; }
    
    /* Botones de peligro y éxito */
    div[data-testid="stButton-delete"] > button:first-child { background-color: #c0392b !important; }
    div[data-testid="stButton-save"] > button:first-child { background-color: #27ae60 !important; }
    
    /* Métricas de KPIs gigantes */
    [data-testid="stMetricValue"] { color: #ffffff !important; font-size: 36px !important; font-weight: 800 !important; }
    [data-testid="stMetricLabel"] { color: #a5a8b5 !important; font-size: 16px !important; font-weight: 500 !important; }
    
    /* MEJORA VISUAL: Pestañas (Tabs) Mega-Destacadas estilo Cápsula */
    div[data-testid="stTabs"] {
        background-color: transparent;
    }
    button[data-baseweb="tab"] {
        background-color: #1e1e2f !important;
        border-radius: 12px 12px 0px 0px !important;
        margin-right: 10px !important;
        padding: 16px 32px !important;
        border: 2px solid #333 !important;
        border-bottom: none !important;
    }
    button[data-baseweb="tab"]:hover { background-color: #2a2a3f !important; }
    button[data-baseweb="tab"][aria-selected="true"] {
        background-color: #0068c9 !important;
        border-color: #0068c9 !important;
    }
    button[data-baseweb="tab"] p {
        font-size: 20px !important; font-weight: 700 !important; color: #a5a8b5 !important; margin: 0px !important;
    }
    button[data-baseweb="tab"][aria-selected="true"] p {
        color: #ffffff !important;
    }
    </style>
    """, unsafe_allow_html=True)

# =====================================================================
# [ETIQUETA: VARIABLES GLOBALES Y CONEXIÓN A BASE DE DATOS]
# =====================================================================
PALETA_CORP = ['#0068c9', '#29b09d', '#ff9f43', '#83c9ff', '#ff2b2b', '#7defa1']
meses_nombres = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]

COORDS_ZONAS = {
    "Papaya Garden": {"lat": 13.4925, "lon": -89.3822}, "La Libertad - Conchalio": {"lat": 13.4900, "lon": -89.3245},
    "La Libertad - Julupe": {"lat": 13.5011, "lon": -89.3300}, "Costa del Sol": {"lat": 13.3039, "lon": -88.9450},
    "OLT ARG": {"lat": 13.4880, "lon": -89.3200}, "La Libertad - Agroferreteria": {"lat": 13.4905, "lon": -89.3210},
    "Servidores Gabriela Mistral": {"lat": 13.7000, "lon": -89.2000}, "Los Blancos": {"lat": 13.3100, "lon": -88.9200},
    "Zaragoza": {"lat": 13.5850, "lon": -89.2890}
}
COORD_DEFAULT = {"lat": 13.6929, "lon": -89.2182}

@st.cache_resource
def get_engine():
    return create_engine(st.secrets["neon_dsn"], pool_pre_ping=True, pool_recycle=300, connect_args={"connect_timeout": 10})

engine = get_engine()

# =====================================================================
# [ETIQUETA: FUNCIONES DE CARGA Y PROCESAMIENTO MATEMÁTICO]
# =====================================================================
@st.cache_data(ttl=300)
def load_data():
    try:
        with engine.connect() as conn:
            conn.execute(text("ROLLBACK"))
            return pd.read_sql(text("SELECT * FROM incidents ORDER BY id ASC"), conn)
    except Exception:
        engine.dispose()
        with engine.connect() as conn:
            return pd.read_sql(text("SELECT * FROM incidents ORDER BY id ASC"), conn)

def calcular_metricas(df_kpi, horas_mes_total):
    if df_kpi.empty: return 0.0, 0.0, 100.0, 0.0, 0, 0.0
    downtime_bruto = df_kpi['duracion_horas'].sum()
    mask_acd = (df_kpi['duracion_horas'] > 0) & (df_kpi['clientes_afectados'] > 0)
    acd = ((df_kpi.loc[mask_acd, 'duracion_horas'] * df_kpi.loc[mask_acd, 'clientes_afectados']).sum() / df_kpi.loc[mask_acd, 'clientes_afectados'].sum()) if mask_acd.any() else 0.0
    
    tiempo_real = 0.0
    v_kpi = df_kpi[df_kpi['conocimiento_tiempos'] == 'Total'].copy()
    if not v_kpi.empty:
        s = pd.to_datetime(v_kpi['fecha_inicio'].astype(str) + ' ' + v_kpi['hora_inicio'].astype(str), errors='coerce')
        e = pd.to_datetime(v_kpi['fecha_fin'].astype(str) + ' ' + v_kpi['hora_fin'].astype(str), errors='coerce')
        m = s.notna() & e.notna() & (s <= e)
        if m.any():
            i = sorted(list(zip(s[m].tolist(), e[m].tolist())), key=lambda x: x[0])
            mg = [list(i[0])]
            for c in i[1:]:
                if c[0] <= mg[-1][1]: mg[-1][1] = max(mg[-1][1], c[1])
                else: mg.append(list(c))
            tiempo_real = sum((end - stp).total_seconds() for stp, end in mg) / 3600.0

    sla_resultante = max(0.0, min(100.0, ((horas_mes_total - tiempo_real) / horas_mes_total) * 100))
    mttr = df_kpi[df_kpi['duracion_horas'] > 0]['duracion_horas'].mean() if not df_kpi[df_kpi['duracion_horas'] > 0].empty else 0.0
    return downtime_bruto, acd, sla_resultante, mttr, int(df_kpi['clientes_afectados'].sum()), (df_kpi['duracion_horas'].max() if not df_kpi.empty else 0.0)

# --- PROCESAMIENTO INICIAL DE DATOS ---
df_total = pd.DataFrame()
try:
    df_total = load_data()
    if not df_total.empty:
        df_total.columns = [c.lower() for c in df_total.columns]
        df_total['fecha_convertida'] = pd.to_datetime(df_total['fecha_inicio'], errors='coerce')
        df_total['mes_nombre'] = df_total['fecha_convertida'].dt.month.map(lambda x: meses_nombres[int(x) - 1] if pd.notnull(x) else None)
        df_total['duracion_horas'] = pd.to_numeric(df_total['duracion_horas'], errors='coerce').fillna(0)
        df_total['clientes_afectados'] = pd.to_numeric(df_total['clientes_afectados'], errors='coerce').fillna(0).astype(int)
except Exception as e:
    st.error(f"⚠️ Error BD: {e}")

# =====================================================================
# [ETIQUETA: SIDEBAR ENRIQUECIDO Y PREPARACIÓN DE DATOS]
# =====================================================================
with st.sidebar:
    st.title("🏢 Centro de Operaciones")
    st.caption("Panel de Control Multinet | Enterprise v6.1")
    mes_seleccionado = st.selectbox("📅 Ciclo de Análisis", meses_nombres, index=datetime.now().month - 1)
    
    # Extraemos el dataframe del mes aquí para poder usar las métricas en la barra lateral
    df_mes_sidebar = df_total[df_total['mes_nombre'] == mes_seleccionado].copy() if not df_total.empty else pd.DataFrame()
    
    st.divider()
    st.markdown("### ⚙️ Herramientas NOC")
    if st.toggle("🔄 Modo TV (Auto-Refresh 60s)"):
        import streamlit.components.v1 as components
        components.html("<meta http-equiv='refresh' content='60'>", height=0)
        st.caption("Pantalla actualizándose automáticamente.")
    
    # MEJORA: Resumen Ejecutivo en la barra lateral para evitar que se vea vacía
    st.divider()
    st.markdown("### 📉 Resumen Ejecutivo")
    if not df_mes_sidebar.empty:
        mes_index_side = meses_nombres.index(mes_seleccionado) + 1
        dias_mes_side = calendar.monthrange(datetime.now().year, mes_index_side)[1]
        _, _, _, mttr_side, cl_side, _ = calcular_metricas(df_mes_sidebar, dias_mes_side * 24)
        
        st.metric("Promedio Resolución", f"{mttr_side:.2f} horas")
        st.metric("Total Afectados", f"{cl_side} clientes")
    else:
        st.info("Sin datos registrados.")

df_mes = df_mes_sidebar

# =====================================================================
# [ETIQUETA: ESTRUCTURA DE PESTAÑAS (TABS)]
# =====================================================================
tab1, tab2, tab3 = st.tabs(["📊 Dashboard de Monitoreo", "📝 Registro Operativo", "🔐 Auditoría de Base de Datos"])

# ---------------------------------------------------------------------
# [ETIQUETA: TAB 1 - DASHBOARD VISUAL OPTIMIZADO]
# ---------------------------------------------------------------------
with tab1:
    st.title(f"Visor de Rendimiento: {mes_seleccionado} {datetime.now().year}")

    if df_mes.empty:
        st.success(f"🟢 Excelente estado: No hay fallas registradas en {mes_seleccionado}.")
    else:
        df_filtrado = df_mes.copy()
        
        # Cálculos de Deltas vs Mes Anterior
        mes_index = meses_nombres.index(mes_seleccionado) + 1
        anio_actual = datetime.now().year
        dias_mes = calendar.monthrange(anio_actual, mes_index)[1]
        
        downtime_total, acd_horas, sla_porcentaje, avg_mttr, cl_imp, max_h = calcular_metricas(df_filtrado, dias_mes * 24)
        delta_m, delta_a, delta_s, delta_dias = None, None, None, None

        if mes_index > 1:
            df_pasado = df_total[df_total['mes_nombre'] == meses_nombres[mes_index - 2]].copy()
            if not df_pasado.empty:
                d_b_p, acd_p, sla_p, mttr_p, _, _ = calcular_metricas(df_pasado, calendar.monthrange(anio_actual, mes_index - 1)[1] * 24)
                if mttr_p > 0: delta_m = f"{avg_mttr - mttr_p:+.1f} horas"
                if acd_p > 0: delta_a = f"{acd_horas - acd_p:+.1f} horas"
                if sla_p > 0: delta_s = f"{sla_porcentaje - sla_p:+.2f}%"
                if d_b_p > 0: delta_dias = f"{(downtime_total / 24.0) - (d_b_p / 24.0):+.1f} días"

        # Bloque de KPIs - Redistribuido en 2 filas para mejor lectura y con tooltips
        st.markdown("### 🎯 Indicadores Clave de Rendimiento (KPIs)")
        
        # Fila 1 de KPIs
        k1, k2, k3 = st.columns(3)
        k1.metric("MTTR", f"{avg_mttr:.2f} horas", delta=delta_m, delta_color="inverse", 
                  help="Tiempo Promedio de Resolución: Promedio de horas empleadas para reparar el servicio tras la notificación de la falla.")
        k2.metric("Disponibilidad", f"{sla_porcentaje:.2f}%", delta=delta_s, 
                  help="Nivel integral de servicio operativo (SLA) basado en las horas del mes.")
        k3.metric("ACD", f"{acd_horas:.2f} horas", delta=delta_a, delta_color="inverse", 
                  help="Promedio de Afectación por Cliente: Promedio estadístico de horas continuas en las que un cliente experimentó interrupción de servicio.")
        
        st.write("") # Espaciador
        
        # Fila 2 de KPIs
        k4, k5, k6 = st.columns(3)
        k4.metric("Falla Crítica", f"{max_h:.2f} horas", 
                  help="La duración en horas del incidente más severo y prolongado registrado en este periodo mensual.")
        k5.metric("Afectados", f"{cl_imp} clientes", 
                  help="Cantidad consolidada de usuarios que experimentaron cortes de servicio.")
        k6.metric("Impacto Acum.", f"{downtime_total / 24.0:.1f} días", delta=delta_dias, delta_color="inverse", 
                  help="Sumatoria global del tiempo de desconexión expresado en la equivalencia de días enteros.")

        st.caption("ℹ️ **Nota sobre Clientes Afectados:** La cantidad de clientes mostrada es una estimación. Cuando no se cuenta con el dato exacto, el sistema usa un valor base para no alterar los promedios.")

        st.divider()
        st.markdown("### 🗺️ Análisis Geoespacial y Causas")
        
        # Mapa ocupando todo el ancho para mejor visualización
        df_mapa = df_filtrado.copy()
        df_mapa['lat'] = df_mapa['zona'].apply(lambda x: COORDS_ZONAS.get(x, COORD_DEFAULT)['lat'])
        df_mapa['lon'] = df_mapa['zona'].apply(lambda x: COORDS_ZONAS.get(x, COORD_DEFAULT)['lon'])
        df_map_agg = df_mapa.groupby(['zona', 'lat', 'lon']).agg(Frecuencia=('id', 'count'), Horas_Down=('duracion_horas', 'sum'), Clientes=('clientes_afectados', 'sum')).reset_index()
        fig_map = px.scatter_mapbox(df_map_agg, lat="lat", lon="lon", hover_name="zona", size="Clientes", color="Horas_Down", color_continuous_scale="Inferno", zoom=9, mapbox_style="carto-darkmatter")
        fig_map.update_layout(margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig_map, use_container_width=True)

        st.markdown("### 📊 Desglose de Afectaciones")
        
        # Fila 1 de gráficas
        col_g1, col_g2 = st.columns(2)
        
        with col_g1:
            causas_cortas = {"Corte de Fibra por Terceros": "Terceros", "Corte de Fibra (No Especificado)": "Fibra", "Caída de Árboles sobre Fibra": "Árboles", "Falla de Energía Comercial": "Energía", "Corrosión en Equipos": "Corrosión", "Daños por Fauna": "Fauna", "Falla de Hardware": "Hardware", "Falla de Configuración": "Configuración", "Saturación de Tráfico": "Saturación", "Saturación en Servidor UNIFI": "Sat. UNIFI", "Falla de Inicio en UNIFI": "Inic. UNIFI", "Mantenimiento Programado": "Mantenimiento", "Vandalismo o Hurto": "Vandalismo", "Condiciones Climáticas": "Clima"}
            df_caus = df_filtrado.groupby('causa_raiz').size().reset_index(name='Alertas')
            df_caus['Causa_Corta'] = df_caus['causa_raiz'].map(lambda x: causas_cortas.get(x, str(x).split()[0]))
            fig_rca = px.pie(df_caus, names='Causa_Corta', values='Alertas', hole=0.4, color_discrete_sequence=PALETA_CORP, title="Causas Principales")
            fig_rca.update_traces(textposition='inside', textinfo='percent+label', textfont_size=14)
            fig_rca.update_layout(showlegend=False, margin=dict(l=0, r=0, t=30, b=0), paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_rca, use_container_width=True)

        with col_g2:
            df_req = df_filtrado.groupby('equipo_afectado').size().reset_index(name='Fallos').sort_values('Fallos', ascending=True)
            fig_eq = px.bar(df_req, x='Fallos', y='equipo_afectado', orientation='h', title="Por Equipo", color_discrete_sequence=['#29b09d'])
            fig_eq.update_layout(margin=dict(l=0, r=0, t=30, b=0), paper_bgcolor="rgba(0,0,0,0)", xaxis_title="", yaxis_title="")
            st.plotly_chart(fig_eq, use_container_width=True)

        # Fila 2 de gráficas
        col_g3, col_g4 = st.columns(2)
        
        with col_g3:
            df_serv = df_filtrado.groupby('servicio').size().reset_index(name='Eventos')
            fig_serv = px.bar(df_serv, x='Eventos', y='servicio', orientation='h', title="Por Servicio", color='servicio', color_discrete_sequence=PALETA_CORP)
            fig_serv.update_layout(showlegend=False, margin=dict(l=0, r=0, t=30, b=0), paper_bgcolor="rgba(0,0,0,0)", xaxis_title="", yaxis_title="")
            st.plotly_chart(fig_serv, use_container_width=True)

        with col_g4:
            df_trend = df_filtrado.groupby('fecha_convertida').size().reset_index(name='Eventos')
            fig_trend = px.area(df_trend, x='fecha_convertida', y='Eventos', title="Tendencia Diaria", color_discrete_sequence=['#0068c9'])
            fig_trend.update_layout(margin=dict(l=0, r=0, t=30, b=0), paper_bgcolor="rgba(0,0,0,0)", xaxis_title="", yaxis_title="")
            st.plotly_chart(fig_trend, use_container_width=True)

# ---------------------------------------------------------------------
# [ETIQUETA: TAB 2 - FORMULARIO INTELIGENTE Y DINÁMICO]
# ---------------------------------------------------------------------
with tab2:
    st.title("📝 Ingreso de Incidencias Operativas")
    
    col_form, col_contexto = st.columns([2, 1], gap="large")
    
    with col_form:
        with st.container(border=True):
            zona = st.text_input("📍 Ubicación (Nodo o Zona)")
            c_s1, c_s2 = st.columns(2)
            servicio = c_s1.selectbox("🌐 Servicio", ["Internet", "Cable TV (CATV)", "IPTV (Mnet+)"])
            categoria = c_s2.selectbox("🏢 Segmento", ["Red Multinet (Troncal)", "Cliente Corporativo"])
            equipo = st.selectbox("🖥️ Equipo Afectado", ["OLT", "RB/Mikrotik", "Switch", "ONU", "Servidor", "Fibra Principal", "Caja NAP", "Mufa", "Splitter", "Sistema UNIFI", "Antenas Ubiquiti"])

            st.divider()
            c_t1, c_t2 = st.columns(2)
            
            # MEJORA: Lógica dinámica para la fecha/hora de INICIO
            with c_t1:
                f_i = st.date_input("📅 Fecha de Inicio")
                # Por defecto apagado (False)
                asignar_hi = st.toggle("🕒 Asignar Hora de Inicio", value=False)
                if asignar_hi:
                    h_i = st.time_input("Hora de Apertura")
                    hora_inicio_final = h_i.strftime("%H:%M:%S")
                else:
                    hora_inicio_final = None
                    st.info("ℹ️ Al no asignar hora de inicio, el sistema no calculará duración.")

            # MEJORA: Lógica dinámica para la fecha/hora de CIERRE
            with c_t2:
                f_f = st.date_input("📅 Fecha de Cierre")
                # Por defecto apagado (False)
                asignar_hf = st.toggle("🕒 Asignar Hora de Cierre", value=False)
                if asignar_hf:
                    h_f = st.time_input("Hora de Cierre")
                    final_h = h_f.strftime("%H:%M:%S")
                else:
                    final_h = None
                    st.info("ℹ️ Al no asignar hora de cierre, la incidencia no calculará duración.")

            duracion = 0
            desc_conocimiento = "Total" if hora_inicio_final and final_h else "Parcial"
            if hora_inicio_final and final_h:
                try:
                    duracion = max(0, round((datetime.combine(f_f, h_f) - datetime.combine(f_i, h_i)).total_seconds() / 3600, 2))
                except: duracion = 0

            st.divider()
            c_f1, c_f2 = st.columns(2)
            
            # MEJORA: Lógica dinámica para mostrar/ocultar selector de clientes
            with c_f1:
                if categoria == "Cliente Corporativo":
                    clientes_form = 1
                    # Mensaje azul confirmando que se asignó 1 cliente automático y no muestra el input numérico
                    st.info("🏢 Segmento Corporativo: Se contabiliza 1 enlace afectado de forma automática.", icon="ℹ️")
                else:
                    clientes_form = st.number_input("👤 Clientes Afectados", min_value=0, step=1)

            with c_f2:
                causa = st.selectbox("🛠️ Causa Raíz", ["Corte de Fibra por Terceros", "Corte de Fibra (No Especificado)", "Caída de Árboles sobre Fibra", "Falla de Energía Comercial", "Corrosión en Equipos", "Daños por Fauna", "Falla de Hardware", "Falla de Configuración", "Falla de Redundancia", "Saturación de Tráfico", "Saturación en Servidor UNIFI", "Falla de Inicio en UNIFI", "Mantenimiento Programado", "Vandalismo o Hurto", "Condiciones Climáticas"])
            
            desc = st.text_area("📝 Descripción Técnica y Detallada del Incidente")

            if st.button("💾 Guardar Registro en Base de Datos", type="primary"):
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            INSERT INTO incidents (worksheet_name, gsheet_id, zona, servicio, categoria, equipo_afectado, fecha_inicio, hora_inicio, fecha_fin, hora_fin, clientes_afectados, causa_raiz, descripcion, duracion_horas, conocimiento_tiempos) 
                            VALUES (:ws, 0, :z, :s, :c, :e, :fi, :hi, :ff, :hf, :cl, :cr, :d, :dur, :con)
                        """), {"ws": f"{meses_nombres[f_i.month - 1]} {f_i.year}", "z": zona, "s": servicio, "c": categoria, "e": equipo, "fi": f_i.strftime("%Y-%m-%d"), "hi": hora_inicio_final, "ff": f_f.strftime("%Y-%m-%d"), "hf": final_h, "cl": int(clientes_form), "cr": causa, "d": desc, "dur": duracion, "con": desc_conocimiento})
                    st.success("✅ Guardado Exitosamente.")
                    time.sleep(1)
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e: st.error(f"Error: {e}")

    with col_contexto:
        st.markdown("#### 🕒 Actividad Reciente")
        st.caption(f"Últimos registros ingresados en {mes_seleccionado}.")
        if df_mes.empty:
            st.info("Aún no hay registros en este mes.")
        else:
            df_reciente = df_mes.tail(5).sort_values(by='id', ascending=False)
            for _, row in df_reciente.iterrows():
                with st.container(border=True):
                    st.markdown(f"**📍 {row['zona']}**")
                    st.caption(f"🔧 {row['causa_raiz'][:25]}... | ⏳ {row['duracion_horas']}h")

# ---------------------------------------------------------------------
# [ETIQUETA: TAB 3 - PANEL DE AUDITORÍA Y BASE DE DATOS]
# ---------------------------------------------------------------------
with tab3:
    st.title("🗂️ Auditoría de Base de Datos")
    
    if df_total.empty: st.info("La base de datos está vacía.")
    else:
        busqueda = st.text_input("🔎 Buscar en registros:", placeholder="Filtrar tabla...")
        df_display = df_mes.copy() if not df_mes.empty else df_total.copy()
        if busqueda: df_display = df_display[df_display.astype(str).apply(lambda x: x.str.contains(busqueda, case=False, na=False)).any(axis=1)]
        
        df_display.insert(0, "Seleccionar", False)
        cols_drop = [c for c in ['fecha_convertida', 'mes_nombre', 'lat', 'lon'] if c in df_display.columns]

        edited_df = st.data_editor(
            df_display.drop(columns=cols_drop, errors='ignore'),
            column_config={"Seleccionar": st.column_config.CheckboxColumn("✔", default=False), "id": None, "worksheet_name": None, "gsheet_id": None},
            use_container_width=True, hide_index=True, key="editor_bd"
        )

        filas_del = edited_df[edited_df["Seleccionar"] == True]
        hay_cambios = not df_display.drop(columns=cols_drop + ['Seleccionar'], errors='ignore').reset_index(drop=True).equals(edited_df.drop(columns=['Seleccionar']).reset_index(drop=True))
        
        if not filas_del.empty or hay_cambios:
            st.divider()
            with st.container(border=True):
                c_info, c_auth = st.columns([3, 2])
                with c_info:
                    st.markdown("#### 🔐 Ejecutar Acciones Protegidas")
                    if not filas_del.empty: st.error(f"Se eliminarán {len(filas_del)} registro(s).")
                    if hay_cambios: st.info("Existen modificaciones pendientes de guardar.")
                with c_auth:
                    pin = st.text_input("🔑 PIN Administrador:", type="password")
                
                if pin == "1010":
                    c_b1, c_b2 = st.columns(2)
                    if not filas_del.empty and c_b1.button("🗑️ Eliminar Definitivo", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for rid in filas_del['id']: conn.execute(text("DELETE FROM incidents WHERE id = :id"), {"id": int(rid)})
                        st.cache_data.clear(); st.rerun()
                    
                    if hay_cambios and c_b2.button("💾 Guardar Modificaciones", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for i, edit_row in edited_df.iterrows():
                                orig_row = df_display.drop(columns=cols_drop + ['Seleccionar'], errors='ignore').iloc[i]
                                if not orig_row.equals(edit_row):
                                    conn.execute(text("UPDATE incidents SET zona=:z, servicio=:s, categoria=:c, equipo_afectado=:e, fecha_inicio=:fi, hora_inicio=:hi, fecha_fin=:ff, hora_fin=:hf, clientes_afectados=:cl, causa_raiz=:cr, descripcion=:d, duracion_horas=:dur, conocimiento_tiempos=:con WHERE id=:id"), {
                                        "z": str(edit_row.get('zona','')), "s": str(edit_row.get('servicio','')), "c": str(edit_row.get('categoria','')), "e": str(edit_row.get('equipo_afectado','')), "fi": str(edit_row.get('fecha_inicio','')), "hi": None if str(edit_row.get('hora_inicio','')) in ["None","N/A",""] else str(edit_row.get('hora_inicio')), "ff": str(edit_row.get('fecha_fin','')), "hf": None if str(edit_row.get('hora_fin','')) in ["None","N/A",""] else str(edit_row.get('hora_fin')), "cl": int(edit_row.get('clientes_afectados',0)), "cr": str(edit_row.get('causa_raiz','')), "d": str(edit_row.get('descripcion','')), "dur": float(edit_row.get('duracion_horas',0)), "con": str(edit_row.get('conocimiento_tiempos','')), "id": int(edit_row['id'])
                                    })
                        st.cache_data.clear(); st.rerun()

        st.write("---")
        st.download_button("📥 Exportar Datos (CSV)", df_mes.to_csv(index=False).encode('utf-8'), "Reporte_NOC.csv", "text/csv")
