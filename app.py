import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time, bcrypt, math, pytz, calendar, json
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors as rl_colors
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
from reportlab.lib.units import cm

# =====================================================================
# CONFIGURACIÓN Y ESTILOS GLOBALES
# =====================================================================
st.set_page_config(page_title="Multinet NOC", layout="wide", page_icon="🌐")
SV_TZ = pytz.timezone('America/El_Salvador')

PALETA_CORP = ['#f15c22', '#1d2c59', '#29b09d', '#ff9f43', '#83c9ff', '#ff2b2b']
MESES = ["Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
CAUSAS_RAIZ = [
    "Corte de Fibra por Terceros","Corte de Fibra (No Especificado)","Caída de Árboles sobre Fibra",
    "Falla de Energía Comercial","Corrosión en Equipos","Daños por Fauna","Falla de Hardware",
    "Falla de Configuración","Falla de Redundancia","Saturación de Tráfico",
    "Saturación en Servidor UNIFI","Falla de Inicio en UNIFI","Mantenimiento Programado",
    "Vandalismo o Hurto","Condiciones Climáticas"
]
CATEGORIAS = ["Red Multinet", "Cliente Corporativo", "Falla Interna (No afecta clientes)"]
COORDS = {
    "Papaya Garden":             [13.4925, -89.3822],
    "La Libertad - Conchalio":  [13.4900, -89.3245],
    "La Libertad - Julupe":     [13.5011, -89.3300],
    "Costa del Sol":            [13.3039, -88.9450],
    "OLT ARG":                  [13.4880, -89.3200],
    "La Libertad - Agroferreteria": [13.4905, -89.3210],
    "Servidores Gabriela Mistral":  [13.7000, -89.2000],
    "Los Blancos":              [13.3100, -88.9200],
    "Zaragoza":                 [13.5850, -89.2890],
}
CAT_INTERNA = "Falla Interna (No afecta clientes)"

st.markdown("""
    <style>
    div.stButton > button {
        border: none !important; outline: none !important; box-shadow: none !important;
        border-radius: 8px; width: 100%; font-weight: 600; transition: all 0.3s ease !important;
    }
    div.stButton > button:hover { transform: translateY(-2px); }
    div[data-testid="stButton-delete"] > button { background-color: #c0392b !important; color: white !important; }
    div[data-testid="stButton-save"] > button  { background-color: #27ae60 !important; color: white !important; }
    div.stButton > button:first-child { background-color: #0068c9 !important; color: white !important; }
    [data-testid="stMetricValue"] { color: #ffffff !important; font-size: 36px !important; font-weight: 800 !important; }
    [data-testid="stMetricLabel"] { color: #a5a8b5 !important; font-size: 16px !important; font-weight: 500 !important; }
    div[data-testid="stTabs"] { background-color: transparent; }
    button[data-baseweb="tab"] {
        background-color: #1e1e2f !important; border-radius: 12px 12px 0 0 !important;
        margin-right: 10px !important; padding: 16px 32px !important;
        border: 2px solid #333 !important; border-bottom: none !important; transition: all 0.3s;
    }
    button[data-baseweb="tab"]:hover { background-color: #2a2a3f !important; }
    button[data-baseweb="tab"][aria-selected="true"] { background-color: #f15c22 !important; border-color: #f15c22 !important; }
    button[data-baseweb="tab"] p { font-size: 18px !important; font-weight: 700 !important; color: #a5a8b5 !important; margin: 0 !important; }
    button[data-baseweb="tab"][aria-selected="true"] p { color: #ffffff !important; }
    </style>
""", unsafe_allow_html=True)

# =====================================================================
# BASE DE DATOS Y AUTENTICACIÓN
# =====================================================================
@st.cache_resource
def get_engine():
    return create_engine(st.secrets["neon_dsn"], pool_pre_ping=True, pool_recycle=300)

engine = get_engine()

def hash_pw(p):  return bcrypt.hashpw(p.encode(), bcrypt.gensalt()).decode()
def check_pw(p, h): return bcrypt.checkpw(p.encode(), h.encode())

def init_db():
    with engine.begin() as conn:
        # Tabla de usuarios
        conn.execute(text("""
            ALTER TABLE users
                ADD COLUMN IF NOT EXISTS failed_attempts INT DEFAULT 0,
                ADD COLUMN IF NOT EXISTS locked_until   TIMESTAMP,
                ADD COLUMN IF NOT EXISTS is_banned      BOOLEAN DEFAULT FALSE
        """))
        
        # Tabla de incidentes (aseguramos estructura base)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS incidents (
                id SERIAL PRIMARY KEY, fecha_inicio DATE, hora_inicio TIME,
                fecha_fin DATE, hora_fin TIME, duracion_horas NUMERIC,
                zona VARCHAR(255), equipo_afectado VARCHAR(255), servicio VARCHAR(255),
                causa_raiz VARCHAR(255), clientes_afectados INT, conocimiento_tiempos VARCHAR(50),
                descripcion TEXT, categoria VARCHAR(255) DEFAULT 'Red Multinet',
                severidad VARCHAR(10) DEFAULT 'P3'
            )
        """))
        
        # Tabla de Historial (Opción 6: Versionado)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS incidents_history (
                id SERIAL PRIMARY KEY,
                incident_id INT,
                username VARCHAR(100),
                timestamp TIMESTAMP,
                changes JSONB
            )
        """))
        
        if conn.execute(text("SELECT count(*) FROM users WHERE username='Admin'")).scalar() == 0:
            conn.execute(text("INSERT INTO users (username,password_hash,role,pregunta,respuesta) VALUES ('Admin',:h,'admin','¿Color favorito?','azul')"), {"h": hash_pw("Areakde5")})

try:
    init_db()
except Exception as e:
    st.error(f"Error DB Inicial: {e}")

@st.cache_data(ttl=300)
def load_data_mes(m_idx, anio):
    end_d = calendar.monthrange(anio, m_idx)[1]
    q = "SELECT * FROM incidents WHERE fecha_inicio >= :s AND fecha_inicio <= :e ORDER BY fecha_inicio ASC"
    try:
        with engine.connect() as conn:
            conn.execute(text("ROLLBACK"))
            return pd.read_sql(text(q), conn, params={
                "s": f"{anio}-{m_idx:02d}-01",
                "e": f"{anio}-{m_idx:02d}-{end_d}"
            })
    except:
        return pd.DataFrame()

# =====================================================================
# NORMALIZACIÓN Y MOTOR DE REGLAS (Opciones 1, 2 y 3)
# =====================================================================
def normalizar_df(df):
    if df.empty:
        return df
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    
    # 1. Normalización automática de datos
    df['fecha_convertida']   = pd.to_datetime(df['fecha_inicio'], errors='coerce')
    df['duracion_horas']     = pd.to_numeric(df['duracion_horas'], errors='coerce').fillna(0).astype(float)
    df['clientes_afectados'] = pd.to_numeric(df['clientes_afectados'], errors='coerce').fillna(0).astype(int)
    
    if 'categoria' not in df.columns:
        df['categoria'] = 'Red Multinet'
        
    # Clasificación automática de fallas internas
    internal_causes = ["Saturación en Servidor UNIFI", "Falla de Inicio en UNIFI", "Falla de Hardware (Redundante)"]
    mask_internal = (df['clientes_afectados'] == 0) | (df['causa_raiz'].isin(internal_causes))
    df.loc[mask_internal, 'categoria'] = CAT_INTERNA

    # 2. & 3. Motor de reglas operativas y Severidad P1-P4
    def aplicar_reglas(row):
        sev = row.get('severidad', 'P3')
        alertas = []
        
        # Incompletitud
        if pd.isna(row.get('hora_inicio')) or pd.isna(row.get('hora_fin')):
            alertas.append("Falta hora inicio/fin")
            
        # Reglas de severidad automática
        if row['categoria'] == CAT_INTERNA:
            sev = 'P4'
            alertas.append("Falla interna detectada")
        elif row['clientes_afectados'] >= 500 or row['duracion_horas'] >= 12:
            sev = 'P1'
        elif row['clientes_afectados'] >= 50:
            sev = 'P2'
        else:
            sev = 'P3'
            
        # Alertas de coherencia
        if row['duracion_horas'] > 24:
            alertas.append("Duración > 24h")
        if row['causa_raiz'] == 'Falla de Energía Comercial':
            alertas.append("Sugerencia: Revisar estado de UPS en nodo")

        return pd.Series([sev, " | ".join(alertas) if alertas else "OK", "Incompleto" if "Falta hora" in " | ".join(alertas) else "Completo"])

    reglas = df.apply(aplicar_reglas, axis=1)
    df['severidad_calc'] = reglas[0]
    df['alertas_inteligentes'] = reglas[1]
    df['data_quality_flag'] = reglas[2]

    return df

# =====================================================================
# CÁLCULO DE KPIs
# =====================================================================
def calc_kpis(df, h_tot):
    base = {
        "db": 0.0, "acd": 0.0, "sla": 100.0,
        "mttr_ext": 0.0, "mttr_int": 0.0,
        "cl": 0, "mh": 0.0,
        "has_incomplete": False,
        "n_internas": 0, "db_int": 0.0,
    }
    if df.empty: return base

    mask_int  = df['categoria'] == CAT_INTERNA
    df_ext    = df[~mask_int].copy()
    df_int    = df[mask_int].copy()

    base["n_internas"]      = len(df_int)
    base["db_int"]          = float(df_int['duracion_horas'].sum())
    base["has_incomplete"]  = (df['data_quality_flag'] == 'Incompleto').any()

    if not df_int.empty:
        iv = df_int[df_int['duracion_horas'] > 0]
        base["mttr_int"] = float(iv['duracion_horas'].mean()) if not iv.empty else 0.0

    if df_ext.empty: return base

    base["db"] = float(df_ext['duracion_horas'].sum())

    m_acd = (df_ext['duracion_horas'] > 0) & (df_ext['clientes_afectados'] > 0)
    if m_acd.any():
        base["acd"] = float(
            (df_ext.loc[m_acd, 'duracion_horas'] * df_ext.loc[m_acd, 'clientes_afectados']).sum()
            / df_ext.loc[m_acd, 'clientes_afectados'].sum()
        )

    v_kpi = df_ext[df_ext['data_quality_flag'] == 'Completo'].copy()
    t_real = 0.0
    if not v_kpi.empty:
        s_dt = pd.to_datetime(v_kpi['fecha_inicio'].astype(str) + ' ' + v_kpi['hora_inicio'].astype(str), errors='coerce')
        e_dt = pd.to_datetime(v_kpi['fecha_fin'].astype(str)   + ' ' + v_kpi['hora_fin'].astype(str),   errors='coerce')
        valid = s_dt.notna() & e_dt.notna() & (s_dt <= e_dt)
        if valid.any():
            intervals = sorted([[s, e] for s, e in zip(s_dt[valid], e_dt[valid])], key=lambda x: x[0])
            merged = [intervals[0]]
            for iv in intervals[1:]:
                if iv[0] <= merged[-1][1]: merged[-1][1] = max(merged[-1][1], iv[1])
                else: merged.append(iv)
            t_real = sum((en - st_).total_seconds() for st_, en in merged) / 3600.0

    base["sla"] = max(0.0, min(100.0, ((h_tot - t_real) / h_tot) * 100)) if h_tot > 0 else 100.0

    ev = df_ext[df_ext['duracion_horas'] > 0]
    base["mttr_ext"] = float(ev['duracion_horas'].mean()) if not ev.empty else 0.0
    base["cl"] = int(df_ext['clientes_afectados'].sum())
    base["mh"] = float(df_ext['duracion_horas'].max()) if not df_ext.empty else 0.0

    return base

def log_audit(action, detail):
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO audit_logs (timestamp,username,action,details) VALUES (:t,:u,:a,:d)"
            ), {"t": datetime.now(SV_TZ).replace(tzinfo=None), "u": st.session_state.username, "a": action, "d": detail})
    except: pass

def generar_pdf(mes, anio, kpis, df):
    pass # (El PDF Generation se mantiene igual, se omite aquí por brevedad pero en tu código debes mantener la función original que pasaste)
    return BytesIO(b"PDF Mock").getvalue()

# =====================================================================
# SISTEMA DE LOGIN
# =====================================================================
for k in ['logged_in','role','username','log_u','log_p','log_err','log_msg']:
    if k not in st.session_state:
        st.session_state[k] = False if k == 'logged_in' else ("" if "log" in k else None)

def do_login():
    u, p = st.session_state.log_u, st.session_state.log_p
    try:
        with engine.begin() as conn:
            ud = conn.execute(text(
                "SELECT id,password_hash,role,failed_attempts,locked_until,is_banned FROM users WHERE username=:u"
            ), {"u": u}).fetchone()
            if ud:
                uid, ph, rol, fa, ldt, ban = ud
                fa = fa or 0
                now_sv = datetime.now(SV_TZ).replace(tzinfo=None)
                if ban:
                    st.session_state.log_err = "❌ Cuenta baneada permanentemente."
                elif ldt and ldt > now_sv:
                    mins = (ldt - now_sv).seconds // 60 + 1
                    st.session_state.log_err = f"⏳ Bloqueado temporalmente. Intente en {mins} min."
                elif check_pw(p, ph):
                    conn.execute(text("UPDATE users SET failed_attempts=0,locked_until=NULL WHERE id=:id"), {"id": uid})
                    st.session_state.update({"logged_in": True, "role": rol, "username": u, "log_err": ""})
                    return
                else:
                    fa += 1
                    if fa >= 6:
                        conn.execute(text("UPDATE users SET is_banned=TRUE,failed_attempts=:f WHERE id=:id"), {"f": fa, "id": uid})
                        st.session_state.log_err = "❌ Cuenta bloqueada permanentemente (6 intentos fallidos)."
                    elif fa % 3 == 0:
                        conn.execute(text("UPDATE users SET locked_until=:dt,failed_attempts=:f WHERE id=:id"), {"dt": now_sv + timedelta(minutes=5), "f": fa, "id": uid})
                        st.session_state.log_err = "⏳ Bloqueado 5 minutos por múltiples intentos fallidos."
                    else:
                        conn.execute(text("UPDATE users SET failed_attempts=:f WHERE id=:id"), {"f": fa, "id": uid})
                        st.session_state.log_err = f"❌ Credenciales incorrectas. Intento {fa}/6."
            else:
                st.session_state.log_err = "❌ Usuario o contraseña incorrectos."
    except Exception as e:
        st.session_state.log_err = f"Error de conexión: {e}"
    st.session_state.log_u = ""
    st.session_state.log_p = ""

if not st.session_state.logged_in:
    st.markdown("<div style='margin-top:10vh;'></div>", unsafe_allow_html=True)
    _, col_center, _ = st.columns([1, 1.2, 1])
    with col_center:
        if st.session_state.log_err:
            st.error(st.session_state.log_err)
            st.session_state.log_err = ""
        with st.container(border=True):
            st.markdown("<h2 style='text-align:center;'>🔐 Acceso NOC Central</h2>", unsafe_allow_html=True)
            st.text_input("Usuario",    key="log_u")
            st.text_input("Contraseña", key="log_p", type="password")
            st.button("Iniciar Sesión", type="primary", on_click=do_login, use_container_width=True)
    st.stop()

# =====================================================================
# SIDEBAR
# =====================================================================
with st.sidebar:
    st.caption(f"Usuario: **{st.session_state.username}** | v14.0 (Enhanced)")
    anio_act = datetime.now(SV_TZ).year
    anios    = sorted(list(set([anio_act+1, anio_act, anio_act-1, anio_act-2])), reverse=True)
    a_sel    = st.selectbox("🗓️ Ciclo Anual",   anios, index=anios.index(anio_act))
    m_sel    = st.selectbox("📅 Ciclo Mensual", MESES, index=datetime.now(SV_TZ).month - 1)
    m_idx    = MESES.index(m_sel) + 1
    d_mes    = calendar.monthrange(a_sel, m_idx)[1]

    df_m = normalizar_df(load_data_mes(m_idx, a_sel))

    st.divider()
    if st.button("🚪 Cerrar Sesión", use_container_width=True):
        st.session_state.clear()
        st.rerun()

# =====================================================================
# PESTAÑAS PRINCIPALES
# =====================================================================
pestanas = ["📊 Dashboard", "📖 Explicación de KPIs", "🔍 Correlación de Fallas"] + (
    ["📝 Ingreso", "🗂️ Auditoría BD"] if st.session_state.role == 'admin' else []
)
tabs = st.tabs(pestanas)

# ─────────────────────────────────────────────
# TAB 0 — DASHBOARD (Con Alertas y Data Quality)
# ─────────────────────────────────────────────
with tabs[0]:
    st.title(f"Visor de Rendimiento: {m_sel} {a_sel}")

    if df_m.empty:
        st.success(f"🟢 Excelente estado: No hay fallas registradas en {m_sel} {a_sel}.")
    else:
        # Opción 4: Alertas Inteligentes
        alertas_activas = df_m[df_m['alertas_inteligentes'] != 'OK']
        if not alertas_activas.empty:
            with st.expander("⚠️ Alertas Inteligentes Detectadas", expanded=True):
                for _, row in alertas_activas.iterrows():
                    st.warning(f"**Alerta en incidente ID {row.get('id', 'N/A')} ({row['fecha_convertida'].date() if pd.notna(row['fecha_convertida']) else 'Sin Fecha'}):** {row['alertas_inteligentes']}")

        # Opción 5: Panel de Calidad de Datos (Data Quality Score)
        st.markdown("### 🧬 Panel de Calidad del Dato")
        col_q1, col_q2, col_q3, col_q4, col_q5 = st.columns(5)
        total_regs = len(df_m)
        completos = len(df_m[df_m['data_quality_flag'] == 'Completo'])
        score = (completos / total_regs) * 100
        internas_pct = (len(df_m[df_m['categoria'] == CAT_INTERNA]) / total_regs) * 100
        
        col_q1.metric("Data Quality Score", f"{score:.1f}%", help="% de registros con campos de tiempo y datos completos")
        col_q2.metric("Total Registros", total_regs)
        col_q3.metric("Completos", completos)
        col_q4.metric("Incompletos", total_regs - completos, delta_color="inverse")
        col_q5.metric("% Fallas Internas", f"{internas_pct:.1f}%")
        st.divider()

        kpis = calc_kpis(df_m, d_mes * 24)

        # ── KPIs ──
        st.markdown("### 🎯 Indicadores Clave de Rendimiento (KPIs)")
        _, k1, k2, k3, _ = st.columns([0.5, 2, 2, 2, 0.5])
        k1.metric("MTTR · Clientes", f"{kpis['mttr_ext']:.2f} hrs")
        k2.metric("Disponibilidad (SLA)", f"{kpis['sla']:.2f}%")
        k3.metric("Afectación Cliente (ACD)", f"{kpis['acd']:.2f} hrs")

        st.write("")
        _, k4, k5, k6, _ = st.columns([0.5, 2, 2, 2, 0.5])
        k4.metric("Falla Crítica", f"{kpis['mh']:.2f} hrs")
        k5.metric("Clientes Afectados", f"{kpis['cl']:,}")
        k6.metric("Impacto Acumulado", f"{kpis['db']/24:.1f} días")

        # ── Tendencia Temporal (Código completado del anterior) ──
        st.markdown("### 📈 Análisis Temporal")
        dt_trend = df_m.copy()
        dt_trend['Dia']  = pd.to_datetime(dt_trend['fecha_convertida']).dt.day
        dt_trend['Tipo'] = dt_trend['categoria'].apply(lambda x: "Interna" if x == CAT_INTERNA else "Externa")
        
        trend_agg = dt_trend.groupby(['Dia', 'Tipo']).size().reset_index(name='Fallos')
        
        # Crear rango completo de días del mes
        all_days = pd.DataFrame({'Dia': range(1, d_mes + 1)})
        trend_agg = pd.merge(all_days, trend_agg, on='Dia', how='left').fillna(0)
        
        fig_trend = px.line(trend_agg, x='Dia', y='Fallos', color='Tipo', markers=True, 
                            color_discrete_sequence=['#f15c22', '#29b09d'], title="Tendencia de Fallas Diarias")
        fig_trend.update_layout(xaxis=dict(tickmode='linear', dtick=1))
        st.plotly_chart(fig_trend, use_container_width=True)

# ─────────────────────────────────────────────
# TAB 1 — EXPLICACIÓN DE KPIs
# ─────────────────────────────────────────────
with tabs[1]:
    st.title("📖 Diccionario y Explicación de KPIs")
    st.markdown("""
    Este panel detalla cómo funciona el motor de cálculo del NOC, qué significa cada métrica y cómo influyen los datos ingresados en el resultado final.
    
    ---
    ### 1. Disponibilidad (SLA - Service Level Agreement)
    **¿Qué es?** Es el porcentaje de tiempo durante el mes que la red estuvo operativa para los clientes externos.
    * **Fórmula:** `((Horas Totales del Mes - Horas Reales de Caída) / Horas Totales del Mes) * 100`
    * **¿Cómo se calcula?** El sistema toma todas las fallas que **NO** son internas, extrae la hora de inicio y fin, y *fusiona los intervalos* para no duplicar tiempos si hay dos fallas simultáneas.
    * **Regla estricta:** Si un registro no tiene hora de inicio o fin (es decir, el *Data Quality* es incompleto), **se excluye** del cálculo para no alterar el porcentaje real.
    
    ### 2. MTTR (Mean Time To Repair)
    **¿Qué es?** Es el tiempo promedio que toma resolver una avería desde que se detecta hasta que se restablece el servicio.
    * **MTTR Clientes:** Promedio de horas de resolución exclusivo de incidentes que afectaron a clientes finales (Fallas externas).
    * **MTTR Interno:** Promedio de resolución de fallas en backbone o infraestructura redundante que no causaron corte de servicio.
    
    ### 3. ACD (Afectación Cliente - Average Customer Downtime)
    **¿Qué es?** Mide el impacto real en el cliente ponderando el tiempo de caída por la cantidad de usuarios afectados.
    * **Fórmula:** `Sumatoria de (Duración de falla * Clientes Afectados) / Total de Clientes Afectados en el mes`
    * **Ejemplo:** Una caída de 10 horas que afecta a 2 personas tiene menor ACD que una caída de 2 horas que afecta a 500 personas.
    
    ### 4. Sistema de Severidad (P1 - P4)
    El **Motor de Reglas** clasifica automáticamente los incidentes así:
    * **P1 (Crítico):** Caídas > 12 horas o afectación masiva (> 500 clientes).
    * **P2 (Alto):** Afectación considerable (> 50 clientes).
    * **P3 (Medio):** Afectaciones menores (Por defecto si hay clientes).
    * **P4 (Bajo):** Fallas internas sin impacto al cliente (`clientes = 0`).
    """)

# ─────────────────────────────────────────────
# TAB 2 — CORRELACIÓN DE FALLAS (Opción 7)
# ─────────────────────────────────────────────
with tabs[2]:
    st.title("🔍 Análisis de Correlación de Fallas")
    st.markdown("Esta sección utiliza matrices y cruces de datos para identificar patrones ocultos en la operatividad de la red.")
    
    if not df_m.empty:
        col_c1, col_c2 = st.columns(2)
        
        with col_c1:
            st.markdown("#### Relación: Zona ↔ Causa Raíz")
            corr_zc = df_m.groupby(['zona', 'causa_raiz']).size().reset_index(name='Frecuencia')
            fig_hm = px.density_heatmap(corr_zc, x='zona', y='causa_raiz', z='Frecuencia', 
                                        color_continuous_scale='Oranges', text_auto=True)
            fig_hm.update_layout(margin=dict(l=0, r=0, b=0, t=30))
            st.plotly_chart(fig_hm, use_container_width=True)
            
        with col_c2:
            st.markdown("#### Relación: Equipo Afectado ↔ Duración (Horas)")
            fig_sc = px.scatter(df_m, x='equipo_afectado', y='duracion_horas', color='severidad_calc', 
                                size='clientes_afectados', hover_name='causa_raiz',
                                color_discrete_map={'P1':'#ff2b2b', 'P2':'#ff9f43', 'P3':'#83c9ff', 'P4':'#29b09d'})
            fig_sc.update_layout(margin=dict(l=0, r=0, b=0, t=30))
            st.plotly_chart(fig_sc, use_container_width=True)
    else:
        st.info("No hay datos suficientes para generar matrices de correlación este mes.")

# ─────────────────────────────────────────────
# TAB 3 & 4 — ADMIN: INGRESO Y AUDITORÍA (Opciones 6)
# ─────────────────────────────────────────────
if st.session_state.role == 'admin':
    with tabs[3]:
        st.title("📝 Ingreso de Incidentes (Con Versionado)")
        st.info("Los cambios realizados aquí crearán automáticamente una copia en la tabla `incidents_history`.")
        
        # Formulario básico reconstruido
        with st.form("form_ingreso"):
            col1, col2, col3 = st.columns(3)
            f_fecha = col1.date_input("Fecha Inicio")
            f_hora = col2.time_input("Hora Inicio")
            f_zona = col3.selectbox("Zona", list(COORDS.keys()))
            
            f_causa = st.selectbox("Causa Raíz", CAUSAS_RAIZ)
            f_clientes = st.number_input("Clientes Afectados (0 = Falla Interna)", min_value=0, value=0)
            f_duracion = st.number_input("Duración (Horas)", min_value=0.0, step=0.5)
            
            submitted = st.form_submit_button("Guardar Incidente")
            if submitted:
                # Al guardar, el motor de reglas se aplicará al recargar, 
                # pero guardamos un registro base en la BD.
                try:
                    with engine.begin() as conn:
                        res = conn.execute(text("""
                            INSERT INTO incidents (fecha_inicio, hora_inicio, zona, causa_raiz, clientes_afectados, duracion_horas)
                            VALUES (:fi, :hi, :z, :cr, :cl, :dh) RETURNING id
                        """), {"fi": f_fecha, "hi": f_hora, "z": f_zona, "cr": f_causa, "cl": f_clientes, "dh": f_duracion})
                        
                        new_id = res.fetchone()[0]
                        
                        # Guardar en Historial (Opción 6)
                        data_json = json.dumps({"zona": f_zona, "causa": f_causa, "clientes": f_clientes})
                        conn.execute(text("""
                            INSERT INTO incidents_history (incident_id, username, timestamp, changes)
                            VALUES (:id, :u, :t, :c)
                        """), {"id": new_id, "u": st.session_state.username, "t": datetime.now(SV_TZ).replace(tzinfo=None), "c": data_json})
                        
                    st.success("✅ Incidente guardado con versionado exitoso.")
                except Exception as e:
                    st.error(f"Error al guardar: {e}")

    with tabs[4]:
        st.title("🗂️ Auditoría de Base de Datos y Versionado")
        
        st.subheader("Historial de Cambios (Versionado)")
        try:
            with engine.connect() as conn:
                history = pd.read_sql(text("SELECT * FROM incidents_history ORDER BY timestamp DESC LIMIT 50"), conn)
            if not history.empty:
                st.dataframe(history, use_container_width=True)
            else:
                st.info("No hay historial de cambios registrados aún.")
        except Exception as e:
            st.error("Error cargando historial. Asegúrate de haber ingresado un registro primero.")
