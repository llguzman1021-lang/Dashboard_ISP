import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import calendar
import time
import bcrypt
import io
from fpdf import FPDF

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
# [ETIQUETA: FUNCIONES DE BASE DE DATOS Y ENCRIPTACIÓN]
# =====================================================================
@st.cache_resource
def get_engine():
    return create_engine(st.secrets["neon_dsn"], pool_pre_ping=True, pool_recycle=300, connect_args={"connect_timeout": 10})

engine = get_engine()

def hash_password(password):
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def check_password(password, hashed):
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

# Inicializar Usuarios y Tablas por defecto si no existen
def init_db():
    with engine.begin() as conn:
        res = conn.execute(text("SELECT count(*) FROM users")).scalar()
        if res == 0:
            admin_hash = hash_password("admin123")
            view_hash = hash_password("view123")
            conn.execute(text("INSERT INTO users (username, password_hash, role, pregunta, respuesta) VALUES ('admin', :hash1, 'admin', '¿Color favorito?', 'azul')"), {"hash1": admin_hash})
            conn.execute(text("INSERT INTO users (username, password_hash, role, pregunta, respuesta) VALUES ('viewer', :hash2, 'viewer', '¿Mascota?', 'perro')"), {"hash2": view_hash})

try:
    init_db()
except Exception as e:
    st.error(f"Error inicializando DB: {e}")

# =====================================================================
# [ETIQUETA: SISTEMA DE LOGIN Y RECUPERACIÓN DB]
# =====================================================================
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.role = None
    st.session_state.username = None

if not st.session_state.logged_in:
    st.markdown("<div style='margin-top: 10vh;'></div>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 1.2, 1])
    with col2:
        with st.container(border=True):
            st.markdown("<h2 style='text-align: center;'>🔐 Acceso NOC Multinet</h2>", unsafe_allow_html=True)
            st.write("")
            user_input = st.text_input("Usuario")
            pass_input = st.text_input("Contraseña", type="password")
            
            if st.button("Iniciar Sesión", type="primary"):
                try:
                    with engine.connect() as conn:
                        user_data = conn.execute(text("SELECT password_hash, role FROM users WHERE username = :u"), {"u": user_input}).fetchone()
                        if user_data and check_password(pass_input, user_data[0]):
                            st.session_state.logged_in = True
                            st.session_state.role = user_data[1]
                            st.session_state.username = user_input
                            st.rerun()
                        else:
                            st.error("Credenciales incorrectas.")
                except Exception as e: st.error(f"Error de conexión: {e}")
            
            with st.expander("¿Olvidó su contraseña?"):
                rec_user = st.text_input("Ingrese su usuario:")
                if rec_user:
                    try:
                        with engine.connect() as conn:
                            u_data = conn.execute(text("SELECT pregunta, respuesta FROM users WHERE username = :u"), {"u": rec_user}).fetchone()
                            if u_data:
                                st.info(f"**Pregunta:** {u_data[0]}")
                                rec_resp = st.text_input("Respuesta:", type="password")
                                if rec_resp:
                                    if rec_resp.strip().lower() == str(u_data[1]).lower():
                                        new_pass = st.text_input("Nueva contraseña:", type="password")
                                        if st.button("Actualizar") and new_pass:
                                            with engine.begin() as c2:
                                                c2.execute(text("UPDATE users SET password_hash = :h WHERE username = :u"), {"h": hash_password(new_pass), "u": rec_user})
                                            st.success("Contraseña actualizada.")
                                    else: st.error("Respuesta incorrecta.")
                            else: st.error("Usuario no existe.")
                    except Exception as e: st.error(f"Error: {e}")
    st.stop()

# =====================================================================
# [ETIQUETA: FUNCIONES DE LOG DE AUDITORÍA Y PDF REPARADO]
# =====================================================================
def log_audit(action, details):
    try:
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO audit_logs (username, action, details) VALUES (:u, :a, :d)"), 
                         {"u": st.session_state.username, "a": action, "d": details})
    except: pass

def generar_pdf_ejecutivo(mes, anio, mttr, sla, acd, clientes, d_total, df_fallas):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(200, 10, txt="MULTINET - REPORTE EJECUTIVO NOC", ln=True, align='C')
    pdf.set_font("Arial", 'I', 12)
    pdf.cell(200, 10, txt=f"Periodo Analizado: {mes} {anio}", ln=True, align='C')
    pdf.ln(10)
    
    pdf.set_font("Arial", 'B', 14)
    pdf.cell(200, 10, txt="1. Indicadores Clave de Rendimiento (KPIs)", ln=True)
    pdf.set_font("Arial", '', 12)
    pdf.cell(200, 8, txt=f"- Disponibilidad de Red (SLA): {sla:.2f}%", ln=True)
    pdf.cell(200, 8, txt=f"- Tiempo Promedio de Resolucion (MTTR): {mttr:.2f} horas", ln=True)
    pdf.cell(200, 8, txt=f"- Afectacion por Cliente (ACD): {acd:.2f} horas", ln=True)
    pdf.cell(200, 8, txt=f"- Total Clientes Afectados: {clientes} usuarios", ln=True)
    pdf.cell(200, 8, txt=f"- Impacto Acumulado en la Red: {d_total:.1f} dias", ln=True)
    pdf.ln(10)
    
    pdf.set_font("Arial", 'B', 14)
    pdf.cell(200, 10, txt="2. Top 5 Zonas mas Afectadas (Horas Caidas)", ln=True)
    pdf.set_font("Arial", '', 12)
    if not df_fallas.empty:
        top_zonas = df_fallas.groupby('zona')['duracion_horas'].sum().nlargest(5)
        for z, h in top_zonas.items():
            pdf.cell(200, 8, txt=f"- {z}: {h:.1f} horas", ln=True)
    else:
        pdf.cell(200, 8, txt="Sin incidencias registradas.", ln=True)
        
    try:
        pdf_bytes = pdf.output()
        if isinstance(pdf_bytes, str):
            return pdf_bytes.encode('latin-1')
        return bytes(pdf_bytes)
    except Exception:
        return pdf.output(dest='S').encode('latin-1')

# =====================================================================
# [ETIQUETA: VARIABLES GLOBALES Y CARGA DE DATOS]
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

@st.cache_data(ttl=60)
def load_data():
    query = "SELECT * FROM incidents ORDER BY id ASC"
    try:
        with engine.connect() as conn:
            conn.execute(text("ROLLBACK"))
            return pd.read_sql(text(query), conn)
    except Exception:
        engine.dispose()
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn)

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

# Procesamiento General
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
# [ETIQUETA: SIDEBAR Y FILTRO MENSUAL]
# =====================================================================
with st.sidebar:
    st.title("🏢 Centro de Operaciones")
    st.caption(f"Usuario: {st.session_state.username} | Enterprise v9.3")
    
    anio_actual = datetime.now().year
    mes_seleccionado = st.selectbox("📅 Ciclo de Análisis Mensual", meses_nombres, index=datetime.now().month - 1)
    mes_index = meses_nombres.index(mes_seleccionado) + 1
    dias_mes = calendar.monthrange(anio_actual, mes_index)[1]
    
    # Filtrar datos del mes seleccionado usando Pandas
    df_mes = df_total[df_total['mes_nombre'] == mes_seleccionado].copy() if not df_total.empty else pd.DataFrame()
    
    st.divider()
    st.markdown("### 📉 Resumen Ejecutivo")
    if not df_mes.empty:
        d_tot, acd_s, sla_s, mttr_side, cl_side, max_h_s = calcular_metricas(df_mes, dias_mes * 24)
        st.metric("Promedio Resolución", f"{mttr_side:.2f} horas")
        st.metric("Total Afectados", f"{cl_side} clientes")
        
        st.divider()
        pdf_data = generar_pdf_ejecutivo(mes_seleccionado, anio_actual, mttr_side, sla_s, acd_s, cl_side, d_tot/24.0, df_mes)
        st.download_button(label="📥 Descargar PDF Ejecutivo", data=pdf_data, file_name=f"Reporte_NOC_{mes_seleccionado}.pdf", mime="application/pdf", use_container_width=True)
    else:
        st.info("Sin datos registrados.")
        
    st.divider()
    if st.button("🚪 Cerrar Sesión", use_container_width=True):
        log_audit("LOGOUT", "Cierre de sesión.")
        st.session_state.logged_in = False
        st.session_state.role = None
        st.rerun()

# =====================================================================
# [ETIQUETA: ESTRUCTURA DE PESTAÑAS SEGÚN ROL]
# =====================================================================
nombres_pestanas = ["📊 Dashboard de Monitoreo"]
if st.session_state.role == 'admin':
    nombres_pestanas.extend(["📝 Registro Operativo", "🔐 Auditoría BD", "👥 Usuarios y Logs"])

tabs = st.tabs(nombres_pestanas)

# ---------------------------------------------------------------------
# [ETIQUETA: TAB 1 - DASHBOARD VISUAL]
# ---------------------------------------------------------------------
with tabs[0]:
    st.title(f"Visor de Rendimiento: {mes_seleccionado} {anio_actual}")

    if df_mes.empty:
        st.success(f"🟢 Excelente estado: No hay fallas registradas en {mes_seleccionado}.")
    else:
        df_filtrado = df_mes.copy()

        downtime_total, acd_horas, sla_porcentaje, avg_mttr, cl_imp, max_h = calcular_metricas(df_filtrado, dias_mes * 24)
        delta_m, delta_a, delta_s, delta_dias = None, None, None, None

        if mes_index > 1:
            mes_pasado_nom = meses_nombres[mes_index - 2]
            df_pasado = df_total[df_total['mes_nombre'] == mes_pasado_nom].copy() if not df_total.empty else pd.DataFrame()
            if not df_pasado.empty:
                dias_pasado = calendar.monthrange(anio_actual, mes_index - 1)[1]
                d_b_p, acd_p, sla_p, mttr_p, _, _ = calcular_metricas(df_pasado, dias_pasado * 24)
                if mttr_p > 0: delta_m = f"{avg_mttr - mttr_p:+.1f} horas"
                if acd_p > 0: delta_a = f"{acd_horas - acd_p:+.1f} horas"
                if sla_p > 0: delta_s = f"{sla_porcentaje - sla_p:+.2f}%"
                if d_b_p > 0: delta_dias = f"{(downtime_total / 24.0) - (d_b_p / 24.0):+.1f} días"

        st.markdown("### 🎯 Indicadores Clave de Rendimiento (KPIs)")
        k1, k2, k3 = st.columns(3)
        k1.metric("MTTR", f"{avg_mttr:.2f} horas", delta=delta_m, delta_color="inverse", help="Tiempo Promedio de Resolución")
        k2.metric("Disponibilidad (SLA)", f"{sla_porcentaje:.2f}%", delta=delta_s, help="Nivel integral de servicio operativo (SLA)")
        k3.metric("Afectación por Cliente (ACD)", f"{acd_horas:.2f} horas", delta=delta_a, delta_color="inverse", help="Promedio estadístico de horas continuas.")
        st.write("") 
        k4, k5, k6 = st.columns(3)
        k4.metric("Falla Crítica", f"{max_h:.2f} horas")
        k5.metric("Afectados", f"{cl_imp} clientes")
        k6.metric("Impacto Acumulado", f"{downtime_total / 24.0:.1f} días", delta=delta_dias, delta_color="inverse")

        st.caption("ℹ️ **Nota sobre Clientes Afectados:** La cantidad de clientes mostrada es una estimación. Cuando no se cuenta con el dato exacto, el sistema usa un valor base para no alterar los promedios.")

        st.divider()
        st.markdown("### 🗺️ Análisis Geoespacial y Causas (Haz Clic en el Pastel)")
        
        col_m1, col_m2 = st.columns([3, 2])
        with col_m1:
            df_mapa = df_filtrado.copy()
            df_mapa['lat'] = df_mapa['zona'].apply(lambda x: COORDS_ZONAS.get(x, COORD_DEFAULT)['lat'])
            df_mapa['lon'] = df_mapa['zona'].apply(lambda x: COORDS_ZONAS.get(x, COORD_DEFAULT)['lon'])
            df_map_agg = df_mapa.groupby(['zona', 'lat', 'lon']).agg(Horas_Down=('duracion_horas', 'sum'), Clientes=('clientes_afectados', 'sum')).reset_index()
            
            fig_map = px.scatter_mapbox(df_map_agg, lat="lat", lon="lon", hover_name="zona", size="Clientes", color="Horas_Down", color_continuous_scale="Inferno", zoom=9, mapbox_style="carto-darkmatter")
            fig_map.add_trace(go.Scattermapbox(mode="lines", lat=[13.5850, 13.4900, 13.3039], lon=[-89.2890, -89.3245, -88.9450], line=dict(width=2, color='rgba(0, 104, 201, 0.5)'), name="Troncal Fibra Sur"))
            fig_map.update_layout(margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig_map, use_container_width=True)

        with col_m2:
            causas_cortas = {"Corte de Fibra por Terceros": "Terceros", "Corte de Fibra (No Especificado)": "Fibra", "Caída de Árboles sobre Fibra": "Árboles", "Falla de Energía Comercial": "Energía", "Corrosión en Equipos": "Corrosión", "Daños por Fauna": "Fauna", "Falla de Hardware": "Hardware", "Falla de Configuración": "Configuración", "Saturación de Tráfico": "Saturación", "Saturación en Servidor UNIFI": "Sat. UNIFI", "Falla de Inicio en UNIFI": "Inic. UNIFI", "Mantenimiento Programado": "Mantenimiento", "Vandalismo o Hurto": "Vandalismo", "Condiciones Climáticas": "Clima"}
            df_caus = df_filtrado.groupby('causa_raiz').size().reset_index(name='Alertas')
            df_caus['Causa_Corta'] = df_caus['causa_raiz'].map(lambda x: causas_cortas.get(x, str(x).split()[0]))
            fig_rca = px.pie(df_caus, names='Causa_Corta', values='Alertas', hole=0.4, color_discrete_sequence=PALETA_CORP)
            fig_rca.update_traces(textposition='inside', textinfo='percent+label', textfont_size=14)
            fig_rca.update_layout(showlegend=False, margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)")
            seleccion_pastel = st.plotly_chart(fig_rca, use_container_width=True, on_select="rerun", selection_mode="points")
        
        if seleccion_pastel and len(seleccion_pastel.selection.point_indices) > 0:
            idx = seleccion_pastel.selection.point_indices[0]
            causa_seleccionada = df_caus.iloc[idx]['Causa_Corta']
            st.info(f"🔍 **Vista en Detalle:** Incidencias por '{causa_seleccionada}'")
            df_det = df_filtrado[df_filtrado['causa_raiz'].map(lambda x: causas_cortas.get(x, str(x).split()[0])) == causa_seleccionada]
            st.dataframe(df_det[['fecha_inicio', 'zona', 'equipo_afectado', 'duracion_horas', 'descripcion']], hide_index=True)

        st.markdown("### 📊 Desglose de Afectaciones y Tendencia")
        col_g1, col_g2 = st.columns(2)
        with col_g1:
            df_req = df_filtrado.groupby('equipo_afectado').size().reset_index(name='Fallos').sort_values('Fallos', ascending=True)
            fig_eq = px.bar(df_req, x='Fallos', y='equipo_afectado', orientation='h', title="Por Equipo", color_discrete_sequence=['#29b09d'])
            fig_eq.update_layout(margin=dict(l=0, r=0, t=30, b=0), paper_bgcolor="rgba(0,0,0,0)", xaxis_title="", yaxis_title="")
            st.plotly_chart(fig_eq, use_container_width=True)

        with col_g2:
            df_trend = df_filtrado.copy()
            df_trend['Dia'] = pd.to_datetime(df_trend['fecha_convertida']).dt.day
            df_t_agg = df_trend.groupby('Dia').size().reset_index(name='Eventos')
            df_t_agg['Mes'] = 'Actual'
            
            if mes_index > 1 and not df_total[df_total['mes_nombre'] == meses_nombres[mes_index - 2]].empty:
                df_p_trend = df_total[df_total['mes_nombre'] == meses_nombres[mes_index - 2]].copy()
                df_p_trend['Dia'] = pd.to_datetime(df_p_trend['fecha_convertida']).dt.day
                df_p_agg = df_p_trend.groupby('Dia').size().reset_index(name='Eventos')
                df_p_agg['Mes'] = 'Anterior'
                df_t_agg = pd.concat([df_t_agg, df_p_agg])

            fig_trend = px.line(df_t_agg, x='Dia', y='Eventos', color='Mes', title="Tendencia Diaria (vs Mes Anterior)", color_discrete_map={"Actual": "#0068c9", "Anterior": "rgba(255,255,255,0.2)"})
            fig_trend.update_traces(fill='tozeroy')
            fig_trend.update_layout(margin=dict(l=0, r=0, t=30, b=0), paper_bgcolor="rgba(0,0,0,0)", xaxis_title="Día del Mes", yaxis_title="")
            st.plotly_chart(fig_trend, use_container_width=True)
            
        st.divider()
        st.markdown("### ⏱️ Línea de Tiempo de Fallas Simultáneas (Gantt)")
        df_gantt = df_filtrado.copy()
        df_gantt['Start'] = pd.to_datetime(df_gantt['fecha_inicio'].astype(str) + ' ' + df_gantt['hora_inicio'].astype(str), errors='coerce')
        df_gantt['End'] = pd.to_datetime(df_gantt['fecha_fin'].astype(str) + ' ' + df_gantt['hora_fin'].astype(str), errors='coerce')
        df_gantt = df_gantt.dropna(subset=['Start', 'End'])
        
        if not df_gantt.empty:
            fig_gantt = px.timeline(df_gantt, x_start="Start", x_end="End", y="zona", color="causa_raiz", color_discrete_sequence=PALETA_CORP)
            fig_gantt.update_yaxes(autorange="reversed")
            fig_gantt.update_layout(margin=dict(l=0, r=0, t=30, b=0), paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_gantt, use_container_width=True)

# ---------------------------------------------------------------------
# [ETIQUETA: TABS DE ADMINISTRADOR]
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
                
                # --- MENSAJES INFO RESTAURADOS ---
                with c_t1:
                    f_i = st.date_input("📅 Fecha de Inicio")
                    asignar_hi = st.toggle("🕒 Asignar Hora de Inicio", value=False)
                    if asignar_hi:
                        h_i = st.time_input("Hora de Apertura")
                        hora_inicio_final = h_i.strftime("%H:%M:%S")
                    else:
                        hora_inicio_final = None
                        st.info("ℹ️ Al no asignar hora de inicio, no se calculará duración.")

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
                    try: duracion = max(0, round((datetime.combine(f_f, h_f) - datetime.combine(f_i, h_i)).total_seconds() / 3600, 2))
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
                        log_audit("INSERT", f"Nueva falla en {zona} ({causa})")
                        st.success("✅ Guardado Exitosamente.")
                        time.sleep(1)
                        st.cache_data.clear(); st.rerun()
                    except Exception as e: st.error(f"Error: {e}")

        with col_contexto:
            st.markdown("#### 🕒 Actividad Reciente")
            if not df_mes.empty:
                df_reciente = df_mes.tail(5).sort_values(by='id', ascending=False)
                for _, row in df_reciente.iterrows():
                    with st.container(border=True):
                        st.markdown(f"**📍 {row['zona']}**")
                        st.caption(f"🔧 {row['causa_raiz'][:25]}... | ⏳ {row['duracion_horas']}h")

    with tabs[2]:
        st.title("🗂️ Auditoría de Base de Datos")
        if df_mes.empty: st.info("No hay datos en el mes.")
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
            
            if not filas_del.empty or hay_cambios:
                c_b1, c_b2 = st.columns(2)
                if not filas_del.empty and c_b1.button("🗑️ Eliminar Seleccionados", type="primary", use_container_width=True):
                    with engine.begin() as conn:
                        for rid in filas_del['id']: 
                            conn.execute(text("DELETE FROM incidents WHERE id = :id"), {"id": int(rid)})
                            log_audit("DELETE", f"Eliminado registro ID {rid}")
                    st.cache_data.clear(); st.rerun()
                
                if hay_cambios and c_b2.button("💾 Guardar Modificaciones", type="primary", use_container_width=True):
                    with engine.begin() as conn:
                        for i, edit_row in edited_df.iterrows():
                            orig_row = df_display.drop(columns=cols_drop + ['Seleccionar'], errors='ignore').iloc[i]
                            if not orig_row.equals(edit_row):
                                conn.execute(text("UPDATE incidents SET zona=:z, servicio=:s, categoria=:c, equipo_afectado=:e, fecha_inicio=:fi, hora_inicio=:hi, fecha_fin=:ff, hora_fin=:hf, clientes_afectados=:cl, causa_raiz=:cr, descripcion=:d, duracion_horas=:dur, conocimiento_tiempos=:con WHERE id=:id"), {
                                    "z": str(edit_row.get('zona','')), "s": str(edit_row.get('servicio','')), "c": str(edit_row.get('categoria','')), "e": str(edit_row.get('equipo_afectado','')), "fi": str(edit_row.get('fecha_inicio','')), "hi": None if str(edit_row.get('hora_inicio','')) in ["None","N/A",""] else str(edit_row.get('hora_inicio')), "ff": str(edit_row.get('fecha_fin','')), "hf": None if str(edit_row.get('hora_fin','')) in ["None","N/A",""] else str(edit_row.get('hora_fin')), "cl": int(edit_row.get('clientes_afectados',0)), "cr": str(edit_row.get('causa_raiz','')), "d": str(edit_row.get('descripcion','')), "dur": float(edit_row.get('duracion_horas',0)), "con": str(edit_row.get('conocimiento_tiempos','')), "id": int(edit_row['id'])
                                })
                                log_audit("UPDATE", f"Modificado registro ID {int(edit_row['id'])}")
                    st.cache_data.clear(); st.rerun()

            # --- NUEVO: SECCIÓN DE EXPORTACIÓN EN TAB 3 ---
            st.divider()
            st.markdown("### 📥 Exportar Reportes")
            c_exp1, c_exp2 = st.columns(2)
            with c_exp1:
                st.download_button("📥 Exportar Datos Crudos (CSV)", df_display.to_csv(index=False).encode('utf-8'), f"Datos_NOC_{mes_seleccionado}.csv", "text/csv", use_container_width=True)
            with c_exp2:
                # Reutilizamos la función del sidebar
                d_tot, acd_s, sla_s, mttr_side, cl_side, max_h_s = calcular_metricas(df_mes, dias_mes * 24)
                pdf_data_tab3 = generar_pdf_ejecutivo(mes_seleccionado, anio_actual, mttr_side, sla_s, acd_s, cl_side, d_tot/24.0, df_mes)
                st.download_button(label="📥 Descargar Reporte Ejecutivo (PDF)", data=pdf_data_tab3, file_name=f"Reporte_NOC_{mes_seleccionado}.pdf", mime="application/pdf", use_container_width=True)

    with tabs[3]:
        st.title("👥 Gestión de Usuarios y Auditoría")
        
        col_users, col_logs = st.columns([1, 2], gap="large")
        
        with col_users:
            st.markdown("#### 👤 Crear Nuevo Usuario")
            with st.container(border=True):
                n_usr = st.text_input("Usuario")
                n_pwd = st.text_input("Contraseña", type="password")
                n_rol = st.selectbox("Rol", ["viewer", "admin"])
                n_pre = st.text_input("Pregunta de Seguridad")
                n_res = st.text_input("Respuesta Secreta")
                
                if st.button("Crear Usuario"):
                    if n_usr and n_pwd:
                        try:
                            with engine.begin() as conn:
                                conn.execute(text("INSERT INTO users (username, password_hash, role, pregunta, respuesta) VALUES (:u, :h, :r, :p, :res)"), 
                                            {"u": n_usr, "h": hash_password(n_pwd), "r": n_rol, "p": n_pre, "res": n_res})
                            st.success(f"Usuario {n_usr} creado exitosamente.")
                        except Exception as e: st.error(f"Error: Es posible que el usuario ya exista.")
        
        with col_logs:
            st.markdown("#### 📜 Registro de Actividad (Logs)")
            try:
                with engine.connect() as conn:
                    logs = pd.read_sql(text("SELECT timestamp as Fecha, username as Usuario, action as Accion, details as Detalles FROM audit_logs ORDER BY id DESC LIMIT 50"), conn)
                st.dataframe(logs, use_container_width=True, hide_index=True)
            except:
                st.info("No hay logs disponibles o la tabla de auditoría no está creada.")
