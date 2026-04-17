import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px
from datetime import datetime, timedelta
import calendar
import time

# =====================================================================
# [ETIQUETA: CONFIGURACIÓN INICIAL Y ESTILOS AVANZADOS]
# =====================================================================
st.set_page_config(page_title="Multinet NOC Analytics", layout="wide", page_icon="🌐")

st.markdown("""
    <style>
    div.stButton > button:first-child { background-color: #0068c9; color: white; border-radius: 8px; width: 100%; font-weight: 600; border: none; transition: all 0.2s; }
    div.stButton > button:first-child:hover { background-color: #0056a3; }
    div[data-testid="stButton-delete"] > button:first-child { background-color: #c0392b !important; }
    div[data-testid="stButton-save"] > button:first-child { background-color: #27ae60 !important; }
    [data-testid="stMetricValue"] { color: #ffffff !important; font-size: 36px !important; font-weight: 800 !important; }
    [data-testid="stMetricLabel"] { color: #a5a8b5 !important; font-size: 16px !important; font-weight: 500 !important; }
    div[data-testid="stTabs"] { background-color: transparent; }
    button[data-baseweb="tab"] { background-color: #1e1e2f !important; border-radius: 12px 12px 0px 0px !important; margin-right: 10px !important; padding: 16px 32px !important; border: 2px solid #333 !important; border-bottom: none !important; }
    button[data-baseweb="tab"]:hover { background-color: #2a2a3f !important; }
    button[data-baseweb="tab"][aria-selected="true"] { background-color: #0068c9 !important; border-color: #0068c9 !important; }
    button[data-baseweb="tab"] p { font-size: 20px !important; font-weight: 700 !important; color: #a5a8b5 !important; margin: 0px !important; }
    button[data-baseweb="tab"][aria-selected="true"] p { color: #ffffff !important; }
    
    /* Centrar login */
    .login-box { max-width: 400px; margin: auto; padding: 30px; background-color: #1e1e2f; border-radius: 15px; border: 1px solid #333; text-align: center;}
    </style>
    """, unsafe_allow_html=True)

# =====================================================================
# [ETIQUETA: SISTEMA DE LOGIN, ROLES Y RECUPERACIÓN NATIVA]
# =====================================================================
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.role = None
    st.session_state.username = None

# Base de datos local de usuarios en memoria (con preguntas de seguridad)
if 'USUARIOS' not in st.session_state:
    st.session_state.USUARIOS = {
        "admin": {"pass": "admin123", "role": "admin", "pregunta": "¿Cuál es tu color favorito?", "respuesta": "azul"},
        "viewer": {"pass": "view123", "role": "viewer", "pregunta": "¿Qué animal es tu mascota?", "respuesta": "perro"}
    }

if not st.session_state.logged_in:
    st.markdown("<div style='margin-top: 10vh;'></div>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 1.2, 1])
    with col2:
        with st.container(border=True):
            st.markdown("<h2 style='text-align: center;'>🔐 Acceso NOC Multinet</h2>", unsafe_allow_html=True)
            st.caption("<div style='text-align: center;'>Ingrese sus credenciales corporativas</div>", unsafe_allow_html=True)
            st.write("")
            user_input = st.text_input("Usuario")
            pass_input = st.text_input("Contraseña", type="password")
            
            if st.button("Iniciar Sesión", type="primary"):
                if user_input in st.session_state.USUARIOS and st.session_state.USUARIOS[user_input]["pass"] == pass_input:
                    st.session_state.logged_in = True
                    st.session_state.role = st.session_state.USUARIOS[user_input]["role"]
                    st.session_state.username = user_input
                    st.rerun()
                else:
                    st.error("Credenciales incorrectas.")
            
            # --- MEJORA: Recuperación de contraseña con Preguntas de Seguridad ---
            with st.expander("¿Olvidó su contraseña?"):
                st.markdown("<small>Recuperación nativa mediante pregunta de seguridad.</small>", unsafe_allow_html=True)
                rec_user = st.text_input("Ingrese su nombre de usuario:")
                
                if rec_user in st.session_state.USUARIOS:
                    pregunta_secreta = st.session_state.USUARIOS[rec_user]["pregunta"]
                    st.info(f"**Pregunta de seguridad:** {pregunta_secreta}")
                    rec_resp = st.text_input("Su respuesta:", type="password")
                    
                    if rec_resp:
                        if rec_resp.strip().lower() == st.session_state.USUARIOS[rec_user]["respuesta"].lower():
                            st.success("✅ Identidad verificada.")
                            new_pass = st.text_input("Escriba su nueva contraseña:", type="password")
                            if st.button("Actualizar Contraseña"):
                                if new_pass:
                                    st.session_state.USUARIOS[rec_user]["pass"] = new_pass
                                    st.success("🎉 Contraseña actualizada con éxito. Ya puede iniciar sesión arriba.")
                                else:
                                    st.error("La contraseña no puede estar vacía.")
                        else:
                            st.error("❌ Respuesta incorrecta.")
                elif rec_user:
                    st.error("Usuario no encontrado en el sistema.")
    st.stop() # Detiene la ejecución si no hay login

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
# [ETIQUETA: OPTIMIZACIÓN SQL POR MES]
# =====================================================================
@st.cache_data(ttl=60)
def load_data_mes(mes_idx, anio):
    # Calcula el primer y último día del mes para filtrar desde SQL
    start = f"{anio}-{mes_idx:02d}-01"
    ultimo_dia = calendar.monthrange(anio, mes_idx)[1]
    end = f"{anio}-{mes_idx:02d}-{ultimo_dia}"
    
    query = "SELECT * FROM incidents WHERE fecha_inicio >= :start AND fecha_inicio <= :end ORDER BY fecha_inicio ASC"
    try:
        with engine.connect() as conn:
            conn.execute(text("ROLLBACK"))
            return pd.read_sql(text(query), conn, params={"start": start, "end": end})
    except Exception:
        engine.dispose()
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn, params={"start": start, "end": end})

def calcular_metricas(df_kpi, horas_rango_total):
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

    sla_resultante = max(0.0, min(100.0, ((horas_rango_total - tiempo_real) / horas_rango_total) * 100)) if horas_rango_total > 0 else 100.0
    mttr = df_kpi[df_kpi['duracion_horas'] > 0]['duracion_horas'].mean() if not df_kpi[df_kpi['duracion_horas'] > 0].empty else 0.0
    return downtime_bruto, acd, sla_resultante, mttr, int(df_kpi['clientes_afectados'].sum()), (df_kpi['duracion_horas'].max() if not df_kpi.empty else 0.0)

# =====================================================================
# [ETIQUETA: SIDEBAR - FILTROS CLÁSICOS Y CERRAR SESIÓN]
# =====================================================================
with st.sidebar:
    st.title("🏢 Centro de Operaciones")
    st.caption(f"Usuario: {st.session_state.username} | Enterprise v8.1")
    
    anio_actual = datetime.now().year
    mes_seleccionado = st.selectbox("📅 Ciclo de Análisis Mensual", meses_nombres, index=datetime.now().month - 1)
    mes_index = meses_nombres.index(mes_seleccionado) + 1
    dias_mes = calendar.monthrange(anio_actual, mes_index)[1]
    
    # Descargar datos optimizados solo del mes
    df_mes = load_data_mes(mes_index, anio_actual)
    if not df_mes.empty:
        df_mes.columns = [c.lower() for c in df_mes.columns]
        df_mes['fecha_convertida'] = pd.to_datetime(df_mes['fecha_inicio'], errors='coerce')
        df_mes['duracion_horas'] = pd.to_numeric(df_mes['duracion_horas'], errors='coerce').fillna(0)
        df_mes['clientes_afectados'] = pd.to_numeric(df_mes['clientes_afectados'], errors='coerce').fillna(0).astype(int)
    
    st.divider()
    st.markdown("### 📉 Resumen Ejecutivo")
    if not df_mes.empty:
        _, _, _, mttr_side, cl_side, _ = calcular_metricas(df_mes, dias_mes * 24)
        st.metric("Promedio Resolución", f"{mttr_side:.2f} horas")
        st.metric("Total Afectados", f"{cl_side} clientes")
    else:
        st.info("Sin datos registrados.")
        
    st.divider()
    if st.button("🚪 Cerrar Sesión", use_container_width=True):
        st.session_state.logged_in = False
        st.session_state.role = None
        st.rerun()

# =====================================================================
# [ETIQUETA: ESTRUCTURA DE PESTAÑAS SEGÚN ROL]
# =====================================================================
nombres_pestanas = ["📊 Dashboard de Monitoreo"]
if st.session_state.role == 'admin':
    nombres_pestanas.extend(["📝 Registro Operativo", "🔐 Auditoría de Base de Datos"])

tabs = st.tabs(nombres_pestanas)

# ---------------------------------------------------------------------
# [ETIQUETA: TAB 1 - DASHBOARD VISUAL + GANTT]
# ---------------------------------------------------------------------
with tabs[0]:
    st.title(f"Visor de Rendimiento: {mes_seleccionado} {anio_actual}")

    if df_mes.empty:
        st.success(f"🟢 Excelente estado: No hay fallas registradas en {mes_seleccionado}.")
    else:
        df_filtrado = df_mes.copy()

        # Cálculos de Deltas vs Mes Anterior recuperando datos solo del mes previo
        downtime_total, acd_horas, sla_porcentaje, avg_mttr, cl_imp, max_h = calcular_metricas(df_filtrado, dias_mes * 24)
        delta_m, delta_a, delta_s, delta_dias = None, None, None, None

        if mes_index > 1:
            df_pasado = load_data_mes(mes_index - 1, anio_actual)
            if not df_pasado.empty:
                df_pasado.columns = [c.lower() for c in df_pasado.columns]
                df_pasado['duracion_horas'] = pd.to_numeric(df_pasado['duracion_horas'], errors='coerce').fillna(0)
                df_pasado['clientes_afectados'] = pd.to_numeric(df_pasado['clientes_afectados'], errors='coerce').fillna(0).astype(int)
                
                dias_pasado = calendar.monthrange(anio_actual, mes_index - 1)[1]
                d_b_p, acd_p, sla_p, mttr_p, _, _ = calcular_metricas(df_pasado, dias_pasado * 24)
                
                if mttr_p > 0: delta_m = f"{avg_mttr - mttr_p:+.1f} horas"
                if acd_p > 0: delta_a = f"{acd_horas - acd_p:+.1f} horas"
                if sla_p > 0: delta_s = f"{sla_porcentaje - sla_p:+.2f}%"
                if d_b_p > 0: delta_dias = f"{(downtime_total / 24.0) - (d_b_p / 24.0):+.1f} días"

        # Bloque de KPIs - 2 filas de 3
        st.markdown("### 🎯 Indicadores Clave de Rendimiento (KPIs)")
        k1, k2, k3 = st.columns(3)
        k1.metric("MTTR", f"{avg_mttr:.2f} horas", delta=delta_m, delta_color="inverse", help="Tiempo Promedio de Resolución: Promedio de horas empleadas para reparar el servicio tras la notificación de la falla.")
        k2.metric("Disponibilidad (SLA)", f"{sla_porcentaje:.2f}%", delta=delta_s, help="Nivel integral de servicio operativo (SLA) basado en las horas del mes.")
        k3.metric("Afectación por Cliente (ACD)", f"{acd_horas:.2f} horas", delta=delta_a, delta_color="inverse", help="Promedio de Afectación por Cliente (ACD): Horas promedio continuas de interrupción.")
        
        st.write("") 
        k4, k5, k6 = st.columns(3)
        k4.metric("Falla Crítica", f"{max_h:.2f} horas", help="La duración del incidente más severo en el periodo.")
        k5.metric("Afectados", f"{cl_imp} clientes", help="Cantidad consolidada de usuarios que experimentaron cortes de servicio.")
        k6.metric("Impacto Acumulado", f"{downtime_total / 24.0:.1f} días", delta=delta_dias, delta_color="inverse", help="Sumatoria global del tiempo de desconexión expresado en días enteros.")

        st.caption("ℹ️ **Nota sobre Clientes Afectados:** La cantidad de clientes mostrada es una estimación. Cuando no se cuenta con el dato exacto, el sistema usa un valor base.")

        st.divider()
        st.markdown("### 🗺️ Análisis Geoespacial y Causas")
        
        # Mapa 
        df_mapa = df_filtrado.copy()
        df_mapa['lat'] = df_mapa['zona'].apply(lambda x: COORDS_ZONAS.get(x, COORD_DEFAULT)['lat'])
        df_mapa['lon'] = df_mapa['zona'].apply(lambda x: COORDS_ZONAS.get(x, COORD_DEFAULT)['lon'])
        df_map_agg = df_mapa.groupby(['zona', 'lat', 'lon']).agg(Frecuencia=('id', 'count'), Horas_Down=('duracion_horas', 'sum'), Clientes=('clientes_afectados', 'sum')).reset_index()
        fig_map = px.scatter_mapbox(df_map_agg, lat="lat", lon="lon", hover_name="zona", size="Clientes", color="Horas_Down", color_continuous_scale="Inferno", zoom=9, mapbox_style="carto-darkmatter")
        fig_map.update_layout(margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig_map, use_container_width=True)

        st.markdown("### 📊 Desglose de Afectaciones")
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
            
        # MEJORA: GRÁFICO DE GANTT DE FALLAS (Línea de Tiempo)
        st.divider()
        st.markdown("### ⏱️ Línea de Tiempo de Fallas Simultáneas (Gantt)")
        df_gantt = df_filtrado.copy()
        # Preparar fechas para Gantt
        df_gantt['Start'] = pd.to_datetime(df_gantt['fecha_inicio'].astype(str) + ' ' + df_gantt['hora_inicio'].astype(str), errors='coerce')
        df_gantt['End'] = pd.to_datetime(df_gantt['fecha_fin'].astype(str) + ' ' + df_gantt['hora_fin'].astype(str), errors='coerce')
        df_gantt = df_gantt.dropna(subset=['Start', 'End'])
        
        if not df_gantt.empty:
            fig_gantt = px.timeline(df_gantt, x_start="Start", x_end="End", y="zona", color="causa_raiz", color_discrete_sequence=PALETA_CORP)
            fig_gantt.update_yaxes(autorange="reversed") # Orden visual de arriba a abajo
            fig_gantt.update_layout(margin=dict(l=0, r=0, t=30, b=0), paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_gantt, use_container_width=True)
        else:
            st.info("No hay suficientes datos con horas exactas de inicio y fin para construir el diagrama de tiempo.")

# ---------------------------------------------------------------------
# [ETIQUETA: TAB 2 Y TAB 3 - SOLO DISPONIBLE PARA ADMIN]
# ---------------------------------------------------------------------
if st.session_state.role == 'admin':
    
    with tabs[1]:
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
                with c_t1:
                    f_i = st.date_input("📅 Fecha de Inicio")
                    asignar_hi = st.toggle("🕒 Asignar Hora de Inicio", value=False)
                    if asignar_hi:
                        h_i = st.time_input("Hora de Apertura")
                        hora_inicio_final = h_i.strftime("%H:%M:%S")
                    else:
                        hora_inicio_final = None
                        st.info("ℹ️ Al no asignar hora, no se calculará duración.")

                with c_t2:
                    f_f = st.date_input("📅 Fecha de Cierre")
                    asignar_hf = st.toggle("🕒 Asignar Hora de Cierre", value=False)
                    if asignar_hf:
                        h_f = st.time_input("Hora de Cierre")
                        final_h = h_f.strftime("%H:%M:%S")
                    else:
                        final_h = None
                        st.info("ℹ️ Al no asignar hora de cierre, no se calculará duración.")

                duracion = 0
                desc_conocimiento = "Total" if hora_inicio_final and final_h else "Parcial"
                if hora_inicio_final and final_h:
                    try:
                        duracion = max(0, round((datetime.combine(f_f, h_f) - datetime.combine(f_i, h_i)).total_seconds() / 3600, 2))
                    except: duracion = 0

                st.divider()
                c_f1, c_f2 = st.columns(2)
                with c_f1:
                    if categoria == "Cliente Corporativo":
                        clientes_form = 1
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
            st.caption(f"Últimos registros ingresados en BD.")
            
            # Usar load_data completo (solo ID, zona, causa y duracion) para el listado global rápido
            try:
                with engine.connect() as conn:
                    df_reciente = pd.read_sql(text("SELECT id, zona, causa_raiz, duracion_horas FROM incidents ORDER BY id DESC LIMIT 5"), conn)
                if df_reciente.empty:
                    st.info("Aún no hay registros.")
                else:
                    for _, row in df_reciente.iterrows():
                        with st.container(border=True):
                            st.markdown(f"**📍 {row['zona']}**")
                            st.caption(f"🔧 {row['causa_raiz'][:25]}... | ⏳ {row['duracion_horas']}h")
            except Exception as e:
                st.info("Aún no hay registros.")

    with tabs[2]:
        st.title("🗂️ Auditoría de Base de Datos")
        
        if df_mes.empty: st.info(f"No hay datos en el mes de {mes_seleccionado}.")
        else:
            busqueda = st.text_input("🔎 Buscar en registros:", placeholder="Filtrar tabla...")
            df_display = df_mes.copy()
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
            
            # ADIOS PIN: Como ya estamos en el rol 'admin', los botones aparecen directo sin pedir PIN adicional.
            if not filas_del.empty or hay_cambios:
                st.divider()
                st.markdown("### 🛠️ Confirmar Acciones")
                c_b1, c_b2 = st.columns(2)
                
                if not filas_del.empty and c_b1.button("🗑️ Eliminar Seleccionados", type="primary", use_container_width=True):
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
            st.download_button("📥 Exportar Datos Actuales (CSV)", df_mes.to_csv(index=False).encode('utf-8'), f"Reporte_NOC_{mes_seleccionado}.csv", "text/csv")
