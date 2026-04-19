import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time, bcrypt, math, pytz, calendar, re
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, date, time as datetime_time
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors as rl_colors
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
from reportlab.lib.units import cm

# =====================================================================
# CONFIGURACIÓN GLOBAL Y CONSTANTES CORPORATIVAS
# =====================================================================
st.set_page_config(page_title="Multinet NOC", layout="wide", page_icon="📊")
SV_TZ = pytz.timezone('America/El_Salvador')

COLOR_PRIMARY   = '#f15c22'
COLOR_SECONDARY = '#1d2c59'
COLOR_TEAL      = '#29b09d'
COLOR_DANGER    = '#ff2b2b'
PALETA_CORP     = (COLOR_PRIMARY, COLOR_SECONDARY, COLOR_TEAL, '#ff9f43', '#83c9ff', COLOR_DANGER)

DEFAULT_ZONAS = (
    ("El Rosario", 13.4886, -89.0256), ("ARG", 13.4880, -89.3200), ("Tepezontes", 13.6214, -89.0125),
    ("La Libertad", 13.4883, -89.3200), ("El Tunco", 13.4930, -89.3830), ("Costa del Sol", 13.3039, -88.9450),
    ("Zacatecoluca", 13.5048, -88.8710), ("Zaragoza", 13.5850, -89.2890), ("Santiago Nonualco", 13.5186, -88.9442),
    ("Rio Mar", 13.4900, -89.3500), ("San Salvador (Central)", 13.6929, -89.2182),
)
DEFAULT_EQUIPOS = (
    "ONT", "Repetidor Wi-Fi", "Antena Ubiquiti", "OLT", "Caja NAP", "Switch", "Fibra Principal",
    "Servidor de Aplicativos", "Servidor DNS", "Servidor Virtual", "Mikrotik Concentrador",
    "Encoder", "Caja Señal TV", "Antena Señal TV"
)
DEFAULT_CAUSAS = (
    ("Corte de Fibra por Terceros", True), ("Corte de Fibra (No Especificado)", False), ("Caída de Árboles sobre Fibra", True),
    ("Falla de Energía Comercial", True), ("Corrosión en Equipos", False), ("Daños por Fauna", True),
    ("Falla de Hardware", False), ("Falla de Configuración", False), ("Falla de Redundancia", False),
    ("Saturación de Tráfico", False), ("Saturación en Servidor", False), ("Mantenimiento Programado", False),
    ("Vandalismo o Hurto", True), ("Condiciones Climáticas", True),
)
DEFAULT_SERVICIOS  = ("Internet", "Cable TV (CATV)", "IPTV (Mnet+)", "Internet/Cable TV", "Aplicativos internos")
DEFAULT_CATEGORIAS = ("Red Multinet", "Cliente Corporativo", "Falla Interna (No afecta clientes)")
CAT_INTERNA = "Falla Interna (No afecta clientes)"
MESES = ("Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre")

st.markdown("""
<style>
div.stButton > button { border: none !important; outline: none !important; box-shadow: none !important; border-radius: 8px; width: 100%; font-weight: 600; transition: all 0.3s ease !important; }
div.stButton > button:hover  { transform: translateY(-2px); }
[data-testid="stMetricValue"] { color: #ffffff !important; font-size: 34px !important; font-weight: 800 !important; }
[data-testid="stMetricLabel"] { color: #a5a8b5 !important; font-size: 14px !important; font-weight: 500 !important; }
[data-testid="stMetricDelta"] svg { width: 20px; height: 20px; }
[data-testid="stMetricDelta"] div { font-size: 13px !important; font-weight: 600 !important; }
button[data-baseweb="tab"] { background-color: #1e1e2f !important; border-radius: 12px 12px 0 0 !important; margin-right: 8px !important; padding: 13px 26px !important; border: 2px solid #333 !important; border-bottom: none !important; transition: all 0.3s; }
button[data-baseweb="tab"]:hover { background-color: #2a2a3f !important; }
button[data-baseweb="tab"][aria-selected="true"] { background-color: #f15c22 !important; border-color: #f15c22 !important; }
button[data-baseweb="tab"] p { font-size: 15px !important; font-weight: 700 !important; color: #a5a8b5 !important; margin: 0 !important; }
button[data-baseweb="tab"][aria-selected="true"] p { color: #ffffff !important; }
.st-emotion-cache-1wivap2 { padding-top: 1.5rem; }
</style>
""", unsafe_allow_html=True)

# ── Inicialización SEGURA de sesión ──
sesion_keys = {
    'form_reset': 0, 'logged_in': False, 'role': '', 'username': '', 
    'log_u': '', 'log_p': '', 'flash_msg': '', 'flash_type': '', 'log_err': ''
}

for k, v in sesion_keys.items():
    if k not in st.session_state:
        st.session_state[k] = v

if st.session_state.get('flash_msg'):
    if st.session_state.get('flash_type') == 'error':
        st.error(st.session_state.get('flash_msg'), icon="❌")
    else:
        st.toast(st.session_state.get('flash_msg'), icon="✅")
    st.session_state['flash_msg'] = ""
    st.session_state['flash_type'] = ""

# =====================================================================
# FUNCIONES DE SEGURIDAD
# =====================================================================
def es_password_segura(password: str) -> bool:
    """Verifica que la contraseña tenga min. 8 caracteres, 1 mayúscula, 1 minúscula, 1 número y 1 carácter especial"""
    if len(password) < 8: return False
    if not re.search(r"[A-Z]", password): return False
    if not re.search(r"[a-z]", password): return False
    if not re.search(r"\d", password): return False
    if not re.search(r"[@$!%*?&]", password): return False
    return True

# =====================================================================
# BASE DE DATOS Y ARQUITECTURA RELACIONAL
# =====================================================================
@st.cache_resource
def get_engine():
    return create_engine(st.secrets["neon_dsn"], pool_pre_ping=True, pool_recycle=300)

engine = get_engine()

def hash_pw(p: str) -> str:
    return bcrypt.hashpw(p.encode(), bcrypt.gensalt()).decode()

def check_pw(p: str, h: str) -> bool:
    return bcrypt.checkpw(p.encode(), h.encode())

def init_db():
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, username VARCHAR(50) UNIQUE, password_hash VARCHAR(255), role VARCHAR(20), failed_attempts INT DEFAULT 0, locked_until TIMESTAMP, is_banned BOOLEAN DEFAULT FALSE)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS audit_logs (id SERIAL PRIMARY KEY, timestamp TIMESTAMP, username VARCHAR(50), action VARCHAR(50), details TEXT)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS cat_zonas (id SERIAL PRIMARY KEY, nombre VARCHAR(150) UNIQUE NOT NULL, lat FLOAT DEFAULT 13.6929, lon FLOAT DEFAULT -89.2182)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS cat_equipos (id SERIAL PRIMARY KEY, nombre VARCHAR(100) UNIQUE NOT NULL)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS cat_causas  (id SERIAL PRIMARY KEY, nombre VARCHAR(150) UNIQUE NOT NULL, es_externa BOOLEAN DEFAULT FALSE)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS cat_servicios (id SERIAL PRIMARY KEY, nombre VARCHAR(100) UNIQUE NOT NULL)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS cmdb_nodos (id SERIAL PRIMARY KEY, zona VARCHAR(150), equipo VARCHAR(100), clientes INT DEFAULT 0, fuente VARCHAR(50) DEFAULT 'Manual', ultima_sincronizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP, UNIQUE(zona, equipo))"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS incidents (
                id SERIAL PRIMARY KEY, zona VARCHAR(150) NOT NULL, subzona VARCHAR(150), afectacion_general BOOLEAN DEFAULT TRUE,
                servicio VARCHAR(100), categoria VARCHAR(100), equipo_afectado VARCHAR(100), estado VARCHAR(20) DEFAULT 'Cerrado',
                inicio_incidente TIMESTAMPTZ NOT NULL, fin_incidente TIMESTAMPTZ, clientes_afectados INT DEFAULT 0, impacto_porcentaje FLOAT DEFAULT 0.0,
                causa_raiz VARCHAR(150), descripcion TEXT, duracion_horas FLOAT, conocimiento_tiempos VARCHAR(50) DEFAULT 'Total',
                deleted_at TIMESTAMPTZ DEFAULT NULL
            )"""))

        if conn.execute(text("SELECT count(*) FROM cat_zonas")).scalar() == 0:
            for n, la, lo in DEFAULT_ZONAS:
                conn.execute(text("INSERT INTO cat_zonas (nombre,lat,lon) VALUES (:n,:lat,:lon) ON CONFLICT DO NOTHING"), {"n": n, "lat": la, "lon": lo})
        if conn.execute(text("SELECT count(*) FROM cat_equipos")).scalar() == 0:
            for e in DEFAULT_EQUIPOS:
                conn.execute(text("INSERT INTO cat_equipos (nombre) VALUES (:n) ON CONFLICT DO NOTHING"), {"n": e})
        if conn.execute(text("SELECT count(*) FROM cat_causas")).scalar() == 0:
            for n, ext in DEFAULT_CAUSAS:
                conn.execute(text("INSERT INTO cat_causas (nombre,es_externa) VALUES (:n,:e) ON CONFLICT DO NOTHING"), {"n": n, "e": ext})
        if conn.execute(text("SELECT count(*) FROM cat_servicios")).scalar() == 0:
            for s in DEFAULT_SERVICIOS:
                conn.execute(text("INSERT INTO cat_servicios (nombre) VALUES (:n) ON CONFLICT DO NOTHING"), {"n": s})
        
        # 🛡️ FIX: Crear el usuario Admin con contraseña segura "Areakde5@"
        if conn.execute(text("SELECT count(*) FROM users WHERE username='Admin'")).scalar() == 0:
            conn.execute(text("INSERT INTO users (username,password_hash,role) VALUES ('Admin',:h,'admin')"), {"h": hash_pw("Areakde5@")})

try: init_db()
except Exception as _e: st.error(f"Error DB Inicialización: {_e}")

@st.cache_data(ttl=300, show_spinner=False)
def get_zonas() -> list:
    try:
        with engine.connect() as c: return [(r[0], r[1], r[2]) for r in c.execute(text("SELECT nombre, lat, lon FROM cat_zonas ORDER BY nombre")).fetchall()]
    except: return list(DEFAULT_ZONAS)

@st.cache_data(ttl=300, show_spinner=False)
def get_cat(tabla: str) -> list:
    try:
        with engine.connect() as c: return [r[0] for r in c.execute(text(f"SELECT nombre FROM {tabla} ORDER BY nombre")).fetchall()]
    except: return []

@st.cache_data(ttl=300, show_spinner=False)
def get_causas_con_flag() -> dict:
    try:
        with engine.connect() as c: return {r[0]: r[1] for r in c.execute(text("SELECT nombre, es_externa FROM cat_causas ORDER BY nombre")).fetchall()}
    except: return {}

def clear_catalog_cache():
    get_zonas.clear()
    get_cat.clear()
    get_causas_con_flag.clear()

@st.cache_data(ttl=300, show_spinner=False)
def get_all_cmdb_nodos() -> dict:
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT zona, equipo, clientes FROM cmdb_nodos")).fetchall()
            return {(str(r[0]).lower(), str(r[1]).lower()): r[2] for r in res}
    except: return {}

def get_clientes_cmdb(zona: str, equipo: str) -> int:
    if zona == "San Salvador (Central)": return 0
    cmdb = get_all_cmdb_nodos()
    for (z, e), clientes in cmdb.items():
        if z in zona.lower() and e == equipo.lower():
            return clientes
    return 0

# =====================================================================
# CARGA Y ENRIQUECIMIENTO
# =====================================================================
@st.cache_data(ttl=60, show_spinner=False)
def load_data_rango(
    fecha_ini: date, fecha_fin: date, include_deleted: bool = False,
    zona_filtro: str = "Todas", serv_filtro: str = "Todos", seg_filtro: str = "Todos"
) -> pd.DataFrame:
    s_date = datetime.combine(fecha_ini, datetime_time(0, 0, 0))
    e_date = datetime.combine(fecha_fin, datetime_time(23, 59, 59))

    conds = [
        "("
        "  (inicio_incidente >= :s AND inicio_incidente <= :e)"
        "  OR (fin_incidente >= :s AND fin_incidente <= :e)"
        "  OR (inicio_incidente <= :s AND fin_incidente >= :e)"
        "  OR (inicio_incidente >= :s AND inicio_incidente <= :e AND estado = 'Abierto')"
        ")"
    ]
    conds.append("deleted_at IS NULL" if not include_deleted else "deleted_at IS NOT NULL")
    if zona_filtro != "Todas": conds.append(f"zona = '{zona_filtro.replace(chr(39), chr(39)*2)}'")
    if serv_filtro != "Todos": conds.append(f"servicio = '{serv_filtro.replace(chr(39), chr(39)*2)}'")
    if seg_filtro != "Todos":  conds.append(f"categoria = '{seg_filtro.replace(chr(39), chr(39)*2)}'")

    q = "SELECT * FROM incidents WHERE " + " AND ".join(conds) + " ORDER BY inicio_incidente ASC"
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(q), conn, params={"s": s_date, "e": e_date})
    except:
        return pd.DataFrame()

def enriquecer(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame()
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    
    if 'inicio_incidente' in df.columns:
        df['inicio_incidente'] = pd.to_datetime(df['inicio_incidente'], errors='coerce', utc=True)
        m_ini = df['inicio_incidente'].notnull()
        if m_ini.any(): df.loc[m_ini, 'inicio_incidente'] = df.loc[m_ini, 'inicio_incidente'].dt.tz_convert(SV_TZ)

    if 'fin_incidente' in df.columns:
        df['fin_incidente'] = pd.to_datetime(df['fin_incidente'], errors='coerce', utc=True)
        m_fin = df['fin_incidente'].notnull()
        if m_fin.any(): df.loc[m_fin, 'fin_incidente'] = df.loc[m_fin, 'fin_incidente'].dt.tz_convert(SV_TZ)

    df['duracion_horas']     = pd.to_numeric(df.get('duracion_horas', 0), errors='coerce').fillna(0.0)
    df['clientes_afectados'] = pd.to_numeric(df.get('clientes_afectados', 0), errors='coerce').fillna(0).astype(int)
    causas_ext_map = get_causas_con_flag()

    def _severidad(r) -> str:
        if r.get('estado') == 'Abierto':                                      return '🚨 CRÍTICA (En Curso)'
        if r.get('categoria') == CAT_INTERNA:                                 return '🟢 P4 (Interna)'
        if r.get('duracion_horas', 0) >= 12 or r.get('clientes_afectados', 0) >= 1000: return '🔴 P1 (Crítica)'
        if r.get('duracion_horas', 0) >= 4  or r.get('clientes_afectados', 0) >= 300:  return '🟠 P2 (Alta)'
        return '🟡 P3 (Media)'

    df['Severidad']     = df.apply(_severidad, axis=1)
    df['es_externa']    = df.get('causa_raiz', pd.Series()).map(lambda x: causas_ext_map.get(x, False))
    df['zona_completa'] = df.apply(lambda r: f"{r.get('zona')} (General)" if r.get('afectacion_general', True) else f"{r.get('zona')} - {r.get('subzona')}", axis=1)
    
    return df

# =====================================================================
# KPIs MATEMÁTICOS
# =====================================================================
def _merge_intervals(intervals: list) -> list:
    if not intervals: return []
    srt = sorted(intervals, key=lambda x: x[0])
    merged = [list(srt[0])]
    for s, e in srt[1:]:
        if s <= merged[-1][1]: merged[-1][1] = max(merged[-1][1], e)
        else: merged.append([s, e])
    return merged

def calc_kpis(df: pd.DataFrame, fecha_ini: date, fecha_fin: date) -> dict:
    h_tot = ((datetime.combine(fecha_fin, datetime_time(23, 59, 59)) - datetime.combine(fecha_ini, datetime_time(0, 0, 0))).total_seconds()) / 3600.0
    rng_s = SV_TZ.localize(datetime.combine(fecha_ini, datetime_time(0, 0, 0)))
    rng_e = SV_TZ.localize(datetime.combine(fecha_fin, datetime_time(23, 59, 59)))

    base = {
        "global": {"sla": 100.0, "total_fallas": 0, "mtbf": h_tot, "db": 0.0, "mh": 0.0, "p1": 0},
        "t1":     {"fallas": 0, "mttr": 0.0, "acd": 0.0, "clientes": 0},
        "t2":     {"fallas": 0, "mttr": 0.0},
        "t3":     {"fallas": 0, "clientes_est": 0},
        "int":    {"fallas": 0, "mttr": 0.0},
        "abiertos": 0, "zonas_sla": {}
    }
    if df.empty: return base

    base["abiertos"] = len(df[df.get('estado') == 'Abierto'])

    df_cerrados = df[(df.get('estado') == 'Cerrado') & df.get('inicio_incidente').notnull()]
    df_int      = df_cerrados[df_cerrados.get('categoria') == CAT_INTERNA]
    df_ext      = df_cerrados[df_cerrados.get('categoria') != CAT_INTERNA]

    base["int"]["fallas"] = len(df_int)
    if not df_int.empty:
        iv = df_int[df_int.get('duracion_horas') > 0]
        base["int"]["mttr"] = float(iv.get('duracion_horas').mean()) if not iv.empty else 0.0

    if df_ext.empty: return base

    base["global"]["total_fallas"] = len(df_ext)
    base["global"]["p1"]           = int((df_ext.get('Severidad') == '🔴 P1 (Crítica)').sum())

    df_t3 = df_ext[df_ext.get('conocimiento_tiempos') != 'Total']
    base["t3"]["fallas"]       = len(df_t3)
    base["t3"]["clientes_est"] = int(df_t3.get('clientes_afectados').sum())

    df_exact = df_ext[(df_ext.get('conocimiento_tiempos') == 'Total') & df_ext.get('fin_incidente').notnull()]

    if not df_exact.empty:
        df_sla = df_exact[df_exact.get('causa_raiz') != 'Mantenimiento Programado']
        for z in df_sla.get('zona').unique():
            df_z    = df_sla[df_sla.get('zona') == z]
            s_cl_z  = df_z['inicio_incidente'].clip(lower=rng_s)
            e_cl_z  = df_z['fin_incidente'].clip(upper=rng_e)
            valid_z = (s_cl_z <= e_cl_z)
            ivs_z   = [[s, e] for s, e in zip(s_cl_z[valid_z], e_cl_z[valid_z])]
            t_down_z = sum((e - s).total_seconds() for s, e in _merge_intervals(ivs_z)) / 3600.0
            base["zonas_sla"][z] = max(0.0, min(100.0, (h_tot - t_down_z) / h_tot * 100))

        base["global"]["db"] = float(df_exact.get('duracion_horas').sum())
        base["global"]["mh"] = float(df_exact.get('duracion_horas').max())

    df_t2 = df_exact[df_exact.get('clientes_afectados') == 0]
    base["t2"]["fallas"] = len(df_t2)
    if not df_t2.empty: base["t2"]["mttr"] = float(df_t2.get('duracion_horas').mean())

    df_t1 = df_exact[df_exact.get('clientes_afectados') > 0]
    base["t1"]["fallas"] = len(df_t1)
    if not df_t1.empty:
        base["t1"]["mttr"]     = float(df_t1.get('duracion_horas').mean())
        base["t1"]["clientes"] = int(df_t1.get('clientes_afectados').sum())
        total_hc = (df_t1.get('duracion_horas') * df_t1.get('clientes_afectados')).sum()
        base["t1"]["acd"] = float(total_hc / base["t1"]["clientes"]) if base["t1"]["clientes"] > 0 else 0.0

    if not df_exact.empty:
        df_sla_global = df_exact[df_exact.get('causa_raiz') != 'Mantenimiento Programado']
        s_cl  = df_sla_global['inicio_incidente'].clip(lower=rng_s)
        e_cl  = df_sla_global['fin_incidente'].clip(upper=rng_e)
        valid = (s_cl <= e_cl)
        ivs   = [[s, e] for s, e in zip(s_cl[valid], e_cl[valid])]
        t_down = sum((e - s).total_seconds() for s, e in _merge_intervals(ivs)) / 3600.0
    else: t_down = 0.0

    base["global"]["sla"]  = max(0.0, min(100.0, (h_tot - t_down) / h_tot * 100)) if h_tot > 0 else 100.0
    base["global"]["mtbf"] = float((h_tot - t_down) / len(df_exact)) if len(df_exact) > 0 else float(h_tot)
    return base

# =====================================================================
# COMPONENTE REUTILIZABLE: GRÁFICOS
# =====================================================================
def dibujar_graficos(df_m: pd.DataFrame):
    if df_m.empty or 'duracion_horas' not in df_m.columns: return
    df_m = df_m.copy()

    st.markdown("### 🗺️ Análisis Geográfico y Temporal")
    zonas_coords = {z[0]: (z[1], z[2]) for z in get_zonas()}
    df_map = df_m.copy()
    df_map['lat'] = df_map.get('zona').map(lambda x: zonas_coords.get(x, (13.6929, -89.2182))[0])
    df_map['lon'] = df_map.get('zona').map(lambda x: zonas_coords.get(x, (13.6929, -89.2182))[1])
    
    agg = (df_map.groupby(['zona_completa','lat','lon'])
           .agg(Horas=('duracion_horas','sum'), Clientes=('clientes_afectados','sum'))
           .reset_index())
    agg['Clientes_sz'] = agg.get('Clientes').clip(lower=1)

    fig_map = px.scatter_mapbox(
        agg, lat="lat", lon="lon", hover_name="zona_completa",
        size="Clientes_sz", color="Horas", color_continuous_scale="Inferno",
        zoom=8.5, mapbox_style="carto-darkmatter",
        labels={"Clientes_sz": "Clientes"}, title="Impacto Geográfico por Nodo"
    )
    fig_map.update_layout(margin=dict(l=0,r=0,t=40,b=0), height=450)
    st.plotly_chart(fig_map, use_container_width=True)

    st.write("")
    dias_map   = {0:'Lunes',1:'Martes',2:'Miércoles',3:'Jueves',4:'Viernes',5:'Sábado',6:'Domingo'}
    dias_orden = list(dias_map.values())
    df_heat = df_m[df_m.get('inicio_incidente'].notnull()].copy()

    if not df_heat.empty:
        df_heat['Día']  = pd.Categorical(
            df_heat['inicio_incidente'].dt.dayofweek.map(dias_map),
            categories=dias_orden, ordered=True)
        df_heat['Hora'] = df_heat['inicio_incidente'].dt.hour
        df_hm = df_heat.groupby(['Día','Hora'], observed=False).size().reset_index(name='Fallas')

        fig_hm = px.density_heatmap(
            df_hm, x='Hora', y='Día', z='Fallas',
            color_continuous_scale='Blues', nbinsx=24,
            title="Mapa de Calor (Concentración de Fallas por Hora)"
        )
        fig_hm.update_layout(
            xaxis=dict(tickmode='linear', tick0=0, dtick=1),
            margin=dict(l=0,r=0,t=40,b=0),
            paper_bgcolor="rgba(0,0,0,0)", height=350
        )
        st.plotly_chart(fig_hm, use_container_width=True)

    st.write("")
    st.divider()
    st.markdown("### 📊 Responsabilidad y Causas Principales")

    c_pie, c_bar = st.columns(2)
    with c_pie:
        df_m['Tipo'] = df_m.get('es_externa').map({True: 'Externa (Fuerza Mayor)', False: 'Interna (Infraestructura / NOC)'})
        agg_r = df_m.groupby('Tipo').size().reset_index(name='Eventos')
        fig_p = px.pie(
            agg_r, names='Tipo', values='Eventos', hole=0.5,
            color_discrete_sequence=[COLOR_TEAL, COLOR_DANGER],
            title="Tasa de Responsabilidad"
        )
        fig_p.update_traces(textinfo='percent', textposition='inside')
        fig_p.update_layout(
            showlegend=True,
            legend=dict(orientation="h", yanchor="top", y=-0.1, xanchor="center", x=0.5),
            margin=dict(l=0,r=0,t=40,b=0), paper_bgcolor="rgba(0,0,0,0)", height=400
        )
        st.plotly_chart(fig_p, use_container_width=True)

    with c_bar:
        dc = (df_m.groupby('causa_raiz').size()
              .reset_index(name='Alertas')
              .sort_values('Alertas', ascending=True)
              .tail(8))
        fig_b = px.bar(
            dc, x='Alertas', y='causa_raiz', orientation='h',
            color_discrete_sequence=[COLOR_PRIMARY], text_auto='.0f',
            title="Top Causas Raíz"
        )
        fig_b.update_traces(textposition='outside')
        fig_b.update_layout(
            margin=dict(l=0,r=0,t=40,b=0), paper_bgcolor="rgba(0,0,0,0)",
            height=400, xaxis_title="", yaxis_title=""
        )
        st.plotly_chart(fig_b, use_container_width=True)

# =====================================================================
# AUDITORÍA Y REPORTES PDF
# =====================================================================
def log_audit(action: str, detail: str):
    try:
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO audit_logs (timestamp,username,action,details) VALUES (:t,:u,:a,:d)"),
                {"t": datetime.now(SV_TZ).replace(tzinfo=None),
                 "u": st.session_state.get("username", "Sistema"),
                 "a": action, "d": detail}
            )
    except: pass

def generar_pdf(label_periodo: str, kpis: dict, df: pd.DataFrame) -> bytes:
    def mk_style(name, size, font='Helvetica', color=rl_colors.black, **kw):
        return ParagraphStyle(name, fontSize=size, fontName=font, textColor=color, **kw)

    s_title  = mk_style('t',  22, 'Helvetica-Bold', rl_colors.HexColor(COLOR_SECONDARY), spaceAfter=2)
    s_sub    = mk_style('s',  11, 'Helvetica',      rl_colors.HexColor('#888888'),        spaceAfter=14)
    s_period = mk_style('pe', 11, 'Helvetica-Bold', rl_colors.HexColor(COLOR_PRIMARY),    spaceAfter=2)
    s_body   = mk_style('b',   9, 'Helvetica',      rl_colors.black,                      spaceAfter=4)
    s_sec    = mk_style('h',  13, 'Helvetica-Bold', rl_colors.HexColor(COLOR_SECONDARY),  spaceBefore=14, spaceAfter=6)

    def tbl_style(hdr_color):
        return TableStyle([
            ('BACKGROUND',    (0,0),  (-1,0),  hdr_color),
            ('TEXTCOLOR',     (0,0),  (-1,0),  rl_colors.white),
            ('FONTNAME',      (0,0),  (-1,0),  'Helvetica-Bold'),
            ('FONTSIZE',      (0,0),  (-1,0),  9),
            ('ROWBACKGROUNDS',(0,1),  (-1,-1), [rl_colors.HexColor('#f5f5f5'), rl_colors.white]),
            ('FONTNAME',      (0,1),  (-1,-1), 'Helvetica'),
            ('FONTSIZE',      (0,1),  (-1,-1), 9),
            ('VALIGN',        (0,0),  (-1,-1), 'MIDDLE'),
            ('GRID',          (0,0),  (-1,-1), 0.4, rl_colors.HexColor('#dddddd')),
            ('ROWHEIGHT',     (0,0),  (-1,-1), 20),
            ('LEFTPADDING',   (0,0),  (-1,-1), 7),
            ('RIGHTPADDING',  (0,0),  (-1,-1), 7),
            ('TEXTCOLOR',     (1,1),  (1,-1),  rl_colors.HexColor(COLOR_PRIMARY)),
            ('FONTNAME',      (1,1),  (1,-1),  'Helvetica-Bold'),
            ('ALIGN',         (1,0),  (1,-1),  'CENTER'),
        ])

    buffer  = BytesIO()
    doc     = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm)
    story   = []
    now_str = datetime.now(SV_TZ).strftime('%d/%m/%Y %H:%M')

    story += [
        Paragraph("MULTINET", s_title),
        Paragraph("Reporte Ejecutivo · Network Operations Center", s_sub),
        HRFlowable(width="100%", thickness=2, color=rl_colors.HexColor(COLOR_PRIMARY), spaceAfter=10),
        Paragraph(f"<b>Periodo:</b> {label_periodo}", s_period),
        Paragraph(f"<b>Generado:</b> {now_str} (hora El Salvador)", s_body),
        Spacer(1, 0.5*cm),
    ]

    story.append(Paragraph("1. Métricas Operativas Principales", s_sec))
    kpi_rows = [
        ['Indicador', 'Valor', 'Descripción'],
        ['SLA Global (Disponibilidad)',   f"{kpis['global']['sla']:.3f}%",           'Intervalos fusionados (Excluye mantenimientos)'],
        ['MTTR Real',                     f"{kpis['t1']['mttr']:.2f} horas",         'Resolución promedio (clientes > 0)'],
        ['ACD Real (Afectación)',         f"{kpis['t1']['acd']:.2f} horas",          'Percepción real del usuario'],
        ['Impacto Acumulado',             f"{(kpis['global']['db']/24):.2f} días",   'Horas totales caídas / 24'],
        ['Clientes Impactados',           f"{kpis['t1']['clientes']:,}",             'Usuarios afectados (T1)'],
        ['MTBF (Estabilidad)',            f"{kpis['global']['mtbf']:.1f} horas",     'Tiempo medio entre fallas'],
        ['Fallas P1 Críticas',            f"{kpis['global']['p1']}",                 '>12 h o >1000 clientes'],
    ]
    t = Table(kpi_rows, colWidths=[5.5*cm, 3*cm, 8.5*cm])
    t.setStyle(tbl_style(rl_colors.HexColor(COLOR_SECONDARY)))
    story += [t, Spacer(1, 0.4*cm)]

    story.append(Paragraph("2. Salud de Datos / Pendientes", s_sec))
    pend_rows = [
        ['Estado', 'Cantidad'],
        ['Fallas Abiertas (En Curso)',           str(kpis['abiertos'])],
        ['Fallas Incompletas (Sin hora exacta)', str(kpis['t3']['fallas'])],
        ['Incidentes internos',                  str(kpis['int']['fallas'])],
    ]
    tp = Table(pend_rows, colWidths=[10*cm, 4*cm])
    tp.setStyle(tbl_style(rl_colors.HexColor(COLOR_TEAL)))
    story += [tp, Spacer(1, 0.4*cm)]

    if not df.empty and 'duracion_horas' in df.columns:
        df_ext = df[(df.get('categoria') != CAT_INTERNA) & (df.get('estado') == 'Cerrado')]
        if not df_ext.empty:
            story.append(Paragraph("3. Zonas con Mayor Afectación", s_sec))
            top_z = df_ext.groupby('zona_completa')['duracion_horas'].sum().nlargest(8).reset_index()
            z_rows = [['Zona / Nodo', 'Horas', 'Días Equiv.']]
            for _, r in top_z.iterrows():
                z_rows.append([str(r['zona_completa']), f"{r['duracion_horas']:.1f} h", f"{r['duracion_horas']/24:.2f}"])
            tz_t = Table(z_rows, colWidths=[9*cm, 4*cm, 4*cm])
            tz_t.setStyle(tbl_style(rl_colors.HexColor(COLOR_TEAL)))
            story += [tz_t, Spacer(1, 0.4*cm)]

    story += [
        Spacer(1, 1*cm),
        HRFlowable(width="100%", thickness=0.5, color=rl_colors.HexColor('#dddddd'), spaceAfter=6),
        Paragraph(f"MULTINET NOC  ·  {now_str}  ·  Documento Confidencial",
                  mk_style('f', 7, 'Helvetica', rl_colors.HexColor('#888888'))),
    ]
    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()

# =====================================================================
# LOGIN SECURE
# =====================================================================
def do_login():
    u, p = st.session_state.log_u, st.session_state.log_p
    try:
        with engine.begin() as conn:
            ud = conn.execute(
                text("SELECT id,password_hash,role,failed_attempts,locked_until,is_banned FROM users WHERE username=:u"),
                {"u": u}
            ).fetchone()
            if ud:
                uid, ph, rol, fa, ldt, ban = ud
                fa     = fa or 0
                now_sv = datetime.now(SV_TZ).replace(tzinfo=None)
                if ban:
                    st.session_state.log_err = "❌ Cuenta baneada permanentemente."
                elif ldt and ldt > now_sv:
                    st.session_state.log_err = f"⏳ Cuenta bloqueada por {(ldt - now_sv).seconds // 60 + 1} min."
                elif check_pw(p, ph):
                    conn.execute(text("UPDATE users SET failed_attempts=0,locked_until=NULL WHERE id=:id"), {"id": uid})
                    st.session_state.update({"logged_in": True, "role": rol, "username": u, "log_err": ""})
                    return
                else:
                    fa += 1
                    if fa >= 6:
                        conn.execute(text("UPDATE users SET is_banned=TRUE,failed_attempts=:f WHERE id=:id"), {"f": fa,"id": uid})
                        st.session_state.log_err = "❌ Cuenta bloqueada permanentemente por seguridad."
                    elif fa % 3 == 0:
                        conn.execute(text("UPDATE users SET locked_until=:dt,failed_attempts=:f WHERE id=:id"), {"dt": now_sv + timedelta(minutes=5), "f": fa, "id": uid})
                        st.session_state.log_err = "⏳ Demasiados intentos. Bloqueado por 5 min."
                    else:
                        conn.execute(text("UPDATE users SET failed_attempts=:f WHERE id=:id"), {"f": fa, "id": uid})
                        st.session_state.log_err = "❌ Credenciales incorrectas."
            else:
                st.session_state.log_err = "❌ Credenciales incorrectas."
    except Exception:
        st.session_state.log_err = "Error de conexión DB."
    st.session_state.log_u = ""
    st.session_state.log_p = ""

# Pantalla de login
if not st.session_state.logged_in:
    st.markdown("<div style='margin-top:15vh;'></div>", unsafe_allow_html=True)
    _, col_c, _ = st.columns([1, 1.2, 1])
    with col_c:
        with st.container(border=True):
            st.markdown("<div style='text-align:center;padding:20px 0 10px 0;'><div style='font-size:46px;'>🔐</div><h2 style='margin:10px 0 4px;color:#fff;font-weight:700;'>Acceso NOC Central</h2></div>", unsafe_allow_html=True)

            if st.session_state.log_err:
                st.error(st.session_state.log_err, icon="⚠️")
                st.session_state.log_err = ""

            st.text_input("Usuario",    key="log_u")
            st.text_input("Contraseña", key="log_p", type="password")
            st.button("Iniciar Sesión", type="primary", on_click=do_login, use_container_width=True)
            st.caption("Contacte al Administrador de Redes para gestionar sus credenciales.")
    st.stop()

# =====================================================================
# SIDEBAR
# =====================================================================
with st.sidebar:
    st.caption(f"👤 **{st.session_state.get('username', 'Usuario')}** ({st.session_state.get('role', 'Viewer').capitalize()})  |  NOC v1.0")
    st.divider()

    anio_act   = datetime.now(SV_TZ).year
    anios_list = sorted({anio_act+1, anio_act, anio_act-1, anio_act-2}, reverse=True)
    idx_anio   = anios_list.index(anio_act) if anio_act in anios_list else 0

    a_sel = st.selectbox("🗓️ Año",  anios_list, index=idx_anio)
    m_sel = st.selectbox("📅 Mes",  MESES, index=datetime.now(SV_TZ).month - 1)
    m_idx = MESES.index(m_sel) + 1

    fecha_ini     = date(a_sel, m_idx, 1)
    fecha_fin     = date(a_sel, m_idx, calendar.monthrange(a_sel, m_idx)[1])
    label_periodo = f"{m_sel} {a_sel}"

    st.divider()
    z_sel   = st.selectbox("🗺️ Zona",     ["Todas"]  + [z[0] for z in get_zonas()])
    srv_sel = st.selectbox("🌐 Servicio", ["Todos"]  + get_cat("cat_servicios"))
    seg_sel = st.selectbox("🏢 Segmento", ["Todos"]  + list(DEFAULT_CATEGORIAS))

    df_m = enriquecer(load_data_rango(fecha_ini, fecha_fin, False, z_sel, srv_sel, seg_sel))

    st.divider()
    if not df_m.empty:
        st.download_button(
            "📥 Descargar Reporte PDF",
            data=generar_pdf(label_periodo, calc_kpis(df_m, fecha_ini, fecha_fin), df_m),
            file_name=f"Reporte_NOC_{m_sel}_{a_sel}.pdf",
            mime="application/pdf",
            use_container_width=True
        )

    st.divider()
    if st.button("🚪 Cerrar Sesión", use_container_width=True):
        log_audit("LOGOUT", "Sesión cerrada.")
        st.session_state.clear()
        st.rerun()

    st.markdown("""
        <div style="margin-top: 50px; text-align: center; color: #666; font-size: 11px;">
            💻 Engineered by<br><b>Luis Salvador Guzmán López</b>
        </div>
    """, unsafe_allow_html=True)

# =====================================================================
# PESTAÑAS POR ROL
# =====================================================================
role  = st.session_state.get('role', 'viewer')
t_idx = 0

if   role == 'admin':   tab_labels = ["📊 Dashboard", "📝 Registrar Evento", "🗂️ Historial y Edición", "⚙️ Configuración"]
elif role == 'auditor': tab_labels = ["📊 Dashboard", "📝 Registrar Evento", "🗂️ Historial y Edición"]
else:                   tab_labels = ["📊 Dashboard"]

tabs = st.tabs(tab_labels)

# ─────────────────────────────────────────────
# TAB 0 — DASHBOARD
# ─────────────────────────────────────────────
with tabs[t_idx]:
    st.title(f"📊 Rendimiento de Red: {label_periodo}")

    if df_m.empty:
        st.success("🟢 Sin incidentes registrados en los filtros seleccionados. ¡La red está al 100%!")
    else:
        p_m_idx = 12 if m_idx == 1 else m_idx - 1
        p_a_sel = a_sel - 1 if m_idx == 1 else a_sel
        p_fi    = date(p_a_sel, p_m_idx, 1)
        p_ff    = date(p_a_sel, p_m_idx, calendar.monthrange(p_a_sel, p_m_idx)[1])
        df_prev = enriquecer(load_data_rango(p_fi, p_ff, False, z_sel, srv_sel, seg_sel))

        kpis      = calc_kpis(df_m, fecha_ini, fecha_fin)
        kpis_prev = calc_kpis(df_prev, p_fi, p_ff) if not df_prev.empty else None

        def _delta(cat, key, divisor=1, fmt="{:+.2f}", suffix=""):
            if not kpis_prev:
                return None
            return f"{fmt.format((kpis[cat][key] - kpis_prev[cat][key]) / divisor)}{' '+suffix if suffix else ''}"

        # ── FALLAS EN CURSO ──
        df_abiertos = df_m[df_m.get('estado') == 'Abierto'] if not df_m.empty else pd.DataFrame()
        if not df_abiertos.empty:
            st.error(f"🚨 Tienes {len(df_abiertos)} Falla(s) en Curso (Tickets Abiertos).", icon="🚨")
            st.dataframe(
                df_abiertos[['zona_completa','equipo_afectado','inicio_incidente','causa_raiz','descripcion']]
                .rename(columns={'zona_completa':'Nodo','equipo_afectado':'Equipo',
                                 'inicio_incidente':'Hora de Caída','causa_raiz':'Diagnóstico'}),
                hide_index=True, use_container_width=True
            )
            st.divider()

        # ── ESTADO GENERAL ──
        st.markdown("### 📈 Estado General de la Red")
        st.caption("*Métricas globales calculadas excluyendo los Mantenimientos Programados.*")

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("SLA Global",          f"{kpis['global']['sla']:.3f}%",
                  delta=_delta('global','sla', fmt="{:+.3f}", suffix="%"), delta_color="normal")
        c2.metric("Total Eventos",        kpis['global']['total_fallas'],
                  delta=_delta('global','total_fallas', fmt="{:+.0f}"), delta_color="inverse")
        c3.metric("MTBF (Estabilidad)",   f"{kpis['global']['mtbf']:.1f} horas",
                  delta=_delta('global','mtbf', fmt="{:+.1f}", suffix="h"), delta_color="normal")
        c4.metric("Impacto Acumulado",    f"{kpis['global']['db']/24:.2f} días",
                  delta=_delta('global','db', divisor=24, fmt="{:+.2f}", suffix="d"), delta_color="inverse")
        c5.metric("Falla Mayor (Pico)",   f"{kpis['global']['mh']:.1f} horas",
                  delta=_delta('global','mh', fmt="{:+.1f}", suffix="h"), delta_color="inverse")

        st.divider()

        # ── SLA GEOGRÁFICO ──
        st.markdown("### 📍 SLA por Zona (Disponibilidad Geográfica)")
        st.caption("*Desglose de disponibilidad por cada nodo principal.*")

        zonas_activas = [z[0] for z in get_zonas()]
        sla_data      = [{"Zona": z, "SLA (%)": kpis['zonas_sla'].get(z, 100.0)} for z in zonas_activas]
        df_sla_geo    = pd.DataFrame(sla_data).sort_values("SLA (%)", ascending=True)

        fig_sla = px.bar(
            df_sla_geo, x="SLA (%)", y="Zona", orientation='h',
            text_auto='.3f', color="SLA (%)",
            color_continuous_scale=["#ff2b2b","#ff9f43","#29b09d"],
            title="Ranking de Estabilidad Geográfica"
        )
        fig_sla.update_layout(
            xaxis=dict(range=[max(0, df_sla_geo["SLA (%)"].min()-2), 100.5]),
            margin=dict(l=0,r=0,t=40,b=0), paper_bgcolor="rgba(0,0,0,0)", height=350
        )
        st.plotly_chart(fig_sla, use_container_width=True)

        st.divider()

        # ── IMPACTO A CLIENTES ──
        st.markdown("### 👥 Impacto Directo a Clientes (Datos Exactos)")
        st.info("💡 **Inteligencia de Datos:** Los valores de Clientes Afectados son calculados utilizando el histórico aprendido por el sistema en eventos previos (CMDB Autónoma).")

        if kpis['t1']['fallas'] == 0:
            st.success("🟢 No hay incidentes masivos cerrados con afectación a clientes en este periodo.")
        else:
            t1a, t1b, t1c, t1d = st.columns(4)
            t1a.metric("Fallas Completas",      kpis['t1']['fallas'],
                       delta=_delta('t1','fallas', fmt="{:+.0f}"), delta_color="inverse")
            t1b.metric("MTTR Real (Resolución)", f"{kpis['t1']['mttr']:.2f} horas",
                       delta=_delta('t1','mttr', fmt="{:+.2f}", suffix="h"), delta_color="inverse")
            t1c.metric("ACD Real (Afectación)",  f"{kpis['t1']['acd']:.2f} horas",
                       delta=_delta('t1','acd', fmt="{:+.2f}", suffix="h"), delta_color="inverse")
            t1d.metric("Clientes Afectados",     f"{kpis['t1']['clientes']:,}",
                       delta=_delta('t1','clientes', fmt="{:+.0f}"), delta_color="inverse")

        st.divider()
        dibujar_graficos(df_m)

t_idx += 1

# ─────────────────────────────────────────────
# TAB 1 — REGISTRAR EVENTO
# ─────────────────────────────────────────────
if role in ('admin', 'auditor'):
    with tabs[t_idx]:
        st.title("📝 Registrar Evento Operativo")
        fk = st.session_state.get('form_reset', 0)

        zonas_form   = [z[0] for z in get_zonas()]
        equipos_form = get_cat("cat_equipos")
        causas_form  = list(get_causas_con_flag().keys())
        servs_form   = get_cat("cat_servicios")

        cf, ccx = st.columns([2, 1], gap="large")
        with cf:
            with st.container(border=True):
                c_z1, c_z2 = st.columns([1,1])
                with c_z1:
                    z_f = st.selectbox("📍 Nodo Principal", zonas_form, key=f"z_{fk}")
                with c_z2:
                    st.markdown("<div style='margin-top:32px;'></div>", unsafe_allow_html=True)
                    ag  = st.toggle("🚨 Falla General (Afecta todo el nodo)", value=True, key=f"ag_{fk}")

                sz    = st.text_input("📍 Sub-zona (Ej. Colonia Escalón)", value="General" if ag else "", key=f"sz_{fk}", disabled=ag)
                sz_db = "General" if ag else sz
                st.divider()

                c1f, c2f = st.columns(2)
                srv_f = c1f.selectbox("🌐 Servicio",  servs_form,         key=f"s_{fk}")
                cat_f = c2f.selectbox("🏢 Segmento",  list(DEFAULT_CATEGORIAS), key=f"c_{fk}")
                eq_f  = st.selectbox("🖥️ Equipo",     equipos_form,       key=f"e_{fk}")

                d_cl = get_clientes_cmdb(z_f, eq_f)

                st.divider()

                if z_f == "San Salvador (Central)":
                    cl_f = 0; imp_pct = 0.0
                    st.number_input("👤 Clientes Afectados", value=0, disabled=True, key=f"cl_{fk}")
                    st.warning("⚠️ Centro de Datos: Equipos Core. No aplica conteo de ONTs.")
                elif cat_f == "Cliente Corporativo":
                    cl_f = 1; imp_pct = 0.0
                    st.number_input("👤 Clientes Afectados", value=1, disabled=True, key=f"cl_{fk}")
                    st.caption("🏢 Corporativo: fijado en 1 enlace.")
                elif cat_f == CAT_INTERNA:
                    cl_f = 0; imp_pct = 0.0
                    st.number_input("👤 Clientes Afectados", value=0, disabled=True, key=f"cl_{fk}")
                    st.caption("🔧 Interna: fijado en 0 clientes.")
                else:
                    cl_f    = st.number_input(f"👤 Clientes Afectados *(sugerencia histórica: {d_cl})*", min_value=0, value=d_cl, step=1, key=f"cl_{fk}")
                    imp_pct = round((cl_f / d_cl) * 100, 2) if d_cl > 0 else 0.0

                st.divider()
                estado_ticket = st.radio("🚦 Estado del Evento", ["Cerrado (Falla Resuelta)", "Abierto (Falla en Curso)"], horizontal=True, key=f"estado_{fk}")
                es_abierto    = "Abierto" in estado_ticket

                ct1, ct2 = st.columns(2)
                with ct1:
                    fi    = st.date_input("📅 Fecha de Inicio", key=f"fi_{fk}")
                    hi_on = st.checkbox("🕒 Conozco la hora exacta de inicio", value=False, key=f"hi_on_{fk}")
                    if hi_on:
                        hi_val = st.time_input("Hora Exacta de Inicio", key=f"hi_val_{fk}")
                    else:
                        hi_val = None
                        st.info("ℹ️ Sin hora exacta → Evento Incompleto.")

                with ct2:
                    if es_abierto:
                        ff = fi; hf_val = None; hf_on = False
                        st.warning("🚨 El ticket se guardará como 'Falla en Curso'.")
                    else:
                        ff    = st.date_input("📅 Fecha de Cierre", key=f"ff_{fk}")
                        hf_on = st.checkbox("🕒 Conozco la hora exacta de cierre", value=False, key=f"hf_on_{fk}")
                        if hf_on:
                            hf_val = st.time_input("Hora Exacta de Cierre", key=f"hf_val_{fk}")
                        else:
                            hf_val = None
                            st.info("ℹ️ Sin hora exacta → Evento Incompleto.")

                dur = 0.0; conocimiento = "Parcial"
                if hi_on and hf_on and (not es_abierto):
                    dt_ini = datetime.combine(fi, hi_val)
                    dt_fin = datetime.combine(ff, hf_val)
                    if dt_fin > dt_ini:
                        dur          = max(0.01, round((dt_fin - dt_ini).total_seconds() / 3600, 2))
                        conocimiento = "Total"

                st.write("")
                cr_f   = st.selectbox("🛠️ Causa Raíz", causas_form, key=f"cr_{fk}")
                desc_f = st.text_area("📝 Descripción del Evento", key=f"desc_{fk}")

                if st.button("💾 Guardar Registro", type="primary"):
                    err = False
                    if (not es_abierto) and hi_on and hf_on and (fi > ff or (fi == ff and hi_val >= hf_val)):
                        st.session_state.flash_msg  = "La fecha/hora de cierre no puede ser anterior al inicio."
                        st.session_state.flash_type = "error"
                        err = True

                    if not err:
                        with st.spinner("Guardando en Base de Datos…"):
                            try:
                                hi_db = hi_val if hi_on else datetime_time(0, 0)
                                idi   = SV_TZ.localize(datetime.combine(fi, hi_db))
                                idf   = None if (es_abierto or not hf_on) else SV_TZ.localize(datetime.combine(ff, hf_val))
                                estado_db = 'Abierto' if es_abierto else 'Cerrado'

                                with engine.begin() as conn:
                                    conn.execute(text("""
                                        INSERT INTO incidents
                                        (zona, subzona, afectacion_general, servicio, categoria, equipo_afectado,
                                         estado, inicio_incidente, fin_incidente, clientes_afectados, impacto_porcentaje,
                                         causa_raiz, descripcion, duracion_horas, conocimiento_tiempos)
                                        VALUES (:z,:sz,:ag,:s,:c,:e,:est,:idi,:idf,:cl,:imp,:cr,:d,:dur,:con)
                                    """), {"z": z_f, "sz": sz_db, "ag": ag, "s": srv_f, "c": cat_f, "e": eq_f,
                                           "est": estado_db, "idi": idi, "idf": idf, "cl": cl_f, "imp": imp_pct,
                                           "cr": cr_f, "d": desc_f, "dur": dur, "con": conocimiento})

                                    if z_f != "San Salvador (Central)":
                                        conn.execute(text("""
                                            INSERT INTO cmdb_nodos (zona, equipo, clientes, fuente) VALUES (:z,:e,:cl,'Manual')
                                            ON CONFLICT (zona, equipo) DO UPDATE SET clientes = EXCLUDED.clientes
                                            WHERE EXCLUDED.clientes > cmdb_nodos.clientes
                                        """), {"z": z_f, "e": eq_f, "cl": cl_f})

                                log_audit("INSERT", f"Falla ({estado_db}) en {z_f}")
                                load_data_rango.clear()
                                get_all_cmdb_nodos.clear()
                                st.session_state['form_reset'] += 1
                                st.session_state.flash_msg  = "✅ Registro guardado exitosamente."
                                st.session_state.flash_type = "success"
                                st.rerun()
                            except Exception as e:
                                st.session_state.flash_msg  = f"Error de BD: {e}"
                                st.session_state.flash_type = "error"
                                st.rerun()

        with ccx:
            st.markdown("#### 🕒 Registros Recientes")
            if not df_m.empty:
                for _, r in df_m.sort_values('id', ascending=False).head(6).iterrows():
                    with st.container(border=True):
                        ico = "🚨" if r.get('estado') == 'Abierto' else ("🔧" if r.get('categoria') == CAT_INTERNA else "📡")
                        st.markdown(f"**{ico} {r.get('zona_completa')}**")
                        st.caption(f"{str(r.get('causa_raiz',''))[:30]}… | 👥 {r.get('clientes_afectados', 0)}")
            else:
                st.info("Sin registros en la tabla.")
    t_idx += 1

# ─────────────────────────────────────────────
# TAB 2 — HISTORIAL Y EDICIÓN
# ─────────────────────────────────────────────
if role in ('admin', 'auditor'):
    with tabs[t_idx]:
        st.markdown("### 🗂️ Historial, Auditoría y Edición")

        # ── CIERRE RÁPIDO DE TICKETS ──
        df_abiertos = df_m[df_m.get('estado') == 'Abierto'] if not df_m.empty else pd.DataFrame()
        if not df_abiertos.empty:
            st.warning("🚨 Tienes fallas en curso. Puedes cerrarlas rápidamente aquí:")
            with st.expander("Cerrar Falla en Curso (Ticket Abierto)", expanded=True):
                with st.form("form_cerrar_ticket"):
                    def _fmt_inicio(ts):
                        try:
                            return ts.strftime('%d/%m/%Y %H:%M') if pd.notnull(ts) else 'Sin hora'
                        except Exception:
                            return 'Sin hora'

                    t_opts = {
                        r['id']: f"ID: {r['id']} | Nodo: {r.get('zona_completa')} | Inicio: {_fmt_inicio(r.get('inicio_incidente'))}"
                        for _, r in df_abiertos.iterrows()
                    }
                    sel_t  = st.selectbox("Selecciona la Falla a Cerrar", options=list(t_opts.keys()), format_func=lambda x: t_opts[x])
                    c_fc1, c_fc2 = st.columns(2)
                    f_fin  = c_fc1.date_input("📅 Fecha de Restablecimiento del Servicio")
                    h_fin  = c_fc2.time_input("🕒 Hora Exacta de Restablecimiento")

                    if st.form_submit_button("Cerrar Ticket", type="primary"):
                        r_orig     = df_abiertos[df_abiertos['id'] == sel_t].iloc[0]
                        ini_dt     = r_orig.get('inicio_incidente')
                        fin_dt_val = SV_TZ.localize(datetime.combine(f_fin, h_fin))

                        if pd.isnull(ini_dt):
                            st.error("❌ Este ticket no tiene hora de inicio registrada. Edítalo manualmente en la tabla.")
                        elif fin_dt_val < ini_dt:
                            st.error("❌ La fecha/hora de cierre no puede ser menor a la de inicio.")
                        else:
                            dur_h = max(0.01, round((fin_dt_val - ini_dt).total_seconds() / 3600, 2))
                            with engine.begin() as conn:
                                conn.execute(
                                    text("UPDATE incidents SET estado='Cerrado', fin_incidente=:f, duracion_horas=:d, conocimiento_tiempos='Total' WHERE id=:id"),
                                    {"f": fin_dt_val, "d": dur_h, "id": sel_t}
                                )
                            log_audit("CLOSE TICKET", f"Ticket ID {sel_t} cerrado exitosamente.")
                            load_data_rango.clear()
                            st.session_state.flash_msg  = "✅ Ticket cerrado correctamente."
                            st.session_state.flash_type = "success"
                            st.rerun()

        st.divider()
        st.markdown("#### Edición Avanzada (Tabla Masiva)")
        
        st.info("💡 **¿Cómo gestionar los datos en esta tabla?** \n\n"
                "✏️ **Para Editar:** Haz doble clic en cualquier celda para modificarla. Al terminar, no olvides presionar el botón azul **'💾 Guardar Ediciones Manuales'** abajo.\n\n"
                "🗑️ **Para Eliminar:** Marca la casilla **'✔'** a la izquierda de las filas que quieres borrar, y presiona el botón rojo **'🗑️ Eliminar Seleccionados'** abajo.")

        papelera = st.toggle("🗑️ Explorar Papelera de Reciclaje")
        if papelera:
            st.warning("Estás viendo registros eliminados. Puedes restaurarlos o destruirlos para siempre.")
            
        df_audit = enriquecer(load_data_rango(fecha_ini, fecha_fin, papelera, "Todas", "Todos", "Todos"))

        if df_audit.empty:
            st.info("No hay datos en el servidor para el periodo.")
        else:
            c_s, c_pg = st.columns([4, 1])
            bq = c_s.text_input("🔎 Buscar:", placeholder="Causa, nodo, equipo…")
            df_d = (df_audit[df_audit.astype(str)
                    .apply(lambda x: x.str.contains(bq, case=False, na=False))
                    .any(axis=1)].copy()
                    if bq else df_audit.copy())

            tot_p   = max(1, math.ceil(len(df_d)/15))
            pg      = c_pg.number_input("Página", 1, tot_p, 1, key="p_bd")
            df_page = df_d.iloc[(pg-1)*15 : pg*15].copy()
            df_page.insert(0, "Sel", False)

            drop_cols = ['deleted_at', 'Severidad', 'zona_completa', 'es_externa', 'impacto_porcentaje']

            ed_df = st.data_editor(
                df_page.drop(columns=drop_cols, errors='ignore'),
                column_config={
                    "Sel":              st.column_config.CheckboxColumn("✔", default=False),
                    "id":               None,
                    "estado":           st.column_config.SelectboxColumn("Estado", options=["Cerrado","Abierto"]),
                    "inicio_incidente": st.column_config.DatetimeColumn("Inicio", format="YYYY-MM-DD HH:mm"),
                    "fin_incidente":    st.column_config.DatetimeColumn("Fin",    format="YYYY-MM-DD HH:mm"),
                },
                use_container_width=True, hide_index=True,
                key="editor_incidentes_v10"
            )

            f_sel  = ed_df[ed_df["Sel"] == True]
            ref_df = df_page.drop(columns=drop_cols + ['Sel'], errors='ignore').reset_index(drop=True)

            def strip_tz(s):
                if pd.api.types.is_datetime64_any_dtype(s):
                    return s.dt.tz_convert(None) if (hasattr(s.dt,'tz') and s.dt.tz) else s
                return s

            ed_cmp  = ed_df.drop(columns=['Sel'], errors='ignore').copy().apply(strip_tz)
            ref_cmp = ref_df.copy().apply(strip_tz)
            h_cam   = not ref_cmp.equals(ed_cmp)

            cb1, cb2 = st.columns(2)

            if not f_sel.empty:
                if papelera:
                    if cb1.button("♻️ Restaurar Seleccionados", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for rid in f_sel['id']:
                                conn.execute(text("UPDATE incidents SET deleted_at=NULL WHERE id=:id"), {"id": int(rid)})
                        log_audit("RESTORE", f"{len(f_sel)} registro(s).")
                        load_data_rango.clear()
                        st.session_state.flash_msg  = "♻️ Registros restaurados."
                        st.session_state.flash_type = "success"
                        st.rerun()
                    
                    if cb2.button("🔥 Destruir Definitivamente", type="secondary", use_container_width=True):
                        with engine.begin() as conn:
                            for rid in f_sel['id']:
                                conn.execute(text("DELETE FROM incidents WHERE id=:id"), {"id": int(rid)})
                        log_audit("HARD DELETE", f"{len(f_sel)} registro(s) borrados permanentemente.")
                        load_data_rango.clear()
                        st.session_state.flash_msg  = "🔥 Registros destruidos para siempre."
                        st.session_state.flash_type = "success"
                        st.rerun()
                else:
                    if cb1.button("🗑️ Eliminar Seleccionados", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for rid in f_sel['id']:
                                conn.execute(text("UPDATE incidents SET deleted_at=CURRENT_TIMESTAMP WHERE id=:id"), {"id": int(rid)})
                        log_audit("DELETE (SOFT)", f"{len(f_sel)} registro(s).")
                        load_data_rango.clear()
                        st.session_state.flash_msg  = "🗑️ Registros movidos a la Papelera de Reciclaje."
                        st.session_state.flash_type = "success"
                        st.rerun()

            if h_cam and (not papelera) and cb2.button("💾 Guardar Ediciones Manuales", type="primary", use_container_width=True):
                fechas_validas = True
                for i, r in ed_df.iterrows():
                    if not strip_tz(ref_df.iloc[i]).equals(strip_tz(r.drop('Sel', errors='ignore'))):
                        if pd.isnull(r.get('inicio_incidente')):
                            fechas_validas = False
                            break

                if not fechas_validas:
                    st.session_state.flash_msg  = "❌ Error: La 'Fecha de Inicio' es obligatoria."
                    st.session_state.flash_type = "error"
                    st.rerun()
                else:
                    with engine.begin() as conn:
                        for i, r in ed_df.iterrows():
                            if not strip_tz(ref_df.iloc[i]).equals(strip_tz(r.drop('Sel', errors='ignore'))):
                                try:
                                    ini_dt = pd.to_datetime(r.get('inicio_incidente'))
                                    if pd.isnull(r.get('fin_incidente')):
                                        fin_dt_sql = None; dur_u = 0.0; con_u = "Parcial"; est_u = "Abierto"
                                    else:
                                        fin_dt     = pd.to_datetime(r.get('fin_incidente'))
                                        fin_dt_sql = fin_dt
                                        dur_u      = max(0.01, round((fin_dt - ini_dt).total_seconds() / 3600, 2))
                                        con_u      = "Total"; est_u = "Cerrado"
                                except:
                                    fin_dt_sql = None; dur_u = 0.0; con_u = "Parcial"; est_u = "Abierto"

                                try:
                                    cl_val = r.get('clientes_afectados', 0)
                                    cl_val = int(float(cl_val)) if pd.notnull(cl_val) and str(cl_val).strip() != '' else 0
                                except:
                                    cl_val = 0

                                conn.execute(text("""
                                    UPDATE incidents SET zona=:z, subzona=:sz, afectacion_general=:ag,
                                        servicio=:s, categoria=:c, equipo_afectado=:e, estado=:est,
                                        inicio_incidente=:idi, fin_incidente=:idf, clientes_afectados=:cl,
                                        causa_raiz=:cr, descripcion=:d, duracion_horas=:dur,
                                        conocimiento_tiempos=:con WHERE id=:id
                                """), {"z": str(r.get('zona','')), "sz": str(r.get('subzona','')),
                                       "ag": bool(r.get('afectacion_general', True)),
                                       "s": str(r.get('servicio','')), "c": str(r.get('categoria','')),
                                       "e": str(r.get('equipo_afectado','')), "est": est_u,
                                       "idi": ini_dt, "idf": fin_dt_sql,
                                       "cl": cl_val,
                                       "cr": str(r.get('causa_raiz','')), "d": str(r.get('descripcion','')),
                                       "dur": dur_u, "con": con_u, "id": int(r.get('id'))})

                    log_audit("UPDATE", "Edición masiva de registros a través de la tabla.")
                    load_data_rango.clear()
                    st.session_state.flash_msg  = "💾 Cambios guardados correctamente."
                    st.session_state.flash_type = "success"
                    st.rerun()

            st.divider()
            st.download_button(
                "📥 Descargar CSV de esta Tabla",
                df_d.drop(columns=drop_cols, errors='ignore').to_csv(index=False).encode(),
                f"NOC_Export_{fecha_ini}_{fecha_fin}.csv", "text/csv",
                use_container_width=True
            )
    t_idx += 1

# ─────────────────────────────────────────────
# TAB 3 — CONFIGURACIÓN
# ─────────────────────────────────────────────
if role == 'admin' and len(tabs) > t_idx:
    with tabs[t_idx]:
        st.markdown("### ⚙️ Configuración del Sistema")
        st.caption("Administra catálogos y usuarios. Los cambios se reflejan inmediatamente en la UI.")

        t_zonas, t_equipos, t_causas, t_usuarios = st.tabs(["🗺️ Zonas", "🖥️ Equipos", "🛠️ Causas", "👤 Usuarios y Accesos"])

        with t_zonas:
            st.markdown("#### Gestión de Zonas / Nodos")
            for z_nombre, z_lat, z_lon in get_zonas():
                c_zn, c_zdel = st.columns([5, 1])
                c_zn.text(f"📍 {z_nombre}  (Lat: {z_lat:.4f}, Lon: {z_lon:.4f})")
                if c_zdel.button("🗑️ Eliminar", key=f"del_z_{z_nombre}"):
                    try:
                        with engine.begin() as conn:
                            conn.execute(text("DELETE FROM cat_zonas WHERE nombre=:n"), {"n": z_nombre})
                        clear_catalog_cache()
                        st.session_state.flash_msg = "🗑️ Zona eliminada."
                        st.rerun()
                    except Exception:
                        st.session_state.flash_msg  = "❌ Error: La zona no puede eliminarse porque está en uso."
                        st.session_state.flash_type = "error"
                        st.rerun()
            st.divider()
            with st.form("form_add_zona", clear_on_submit=True):
                st.markdown("**➕ Agregar Nueva Zona**")
                nz  = st.text_input("Nombre del Nodo Principal")
                cla, clo = st.columns(2)
                nlat = cla.number_input("Latitud",  value=13.6929, format="%.4f")
                nlon = clo.number_input("Longitud", value=-89.2182, format="%.4f")
                if st.form_submit_button("Agregar Zona") and nz:
                    try:
                        with engine.begin() as conn:
                            conn.execute(text("INSERT INTO cat_zonas (nombre,lat,lon) VALUES (:n,:la,:lo)"), {"n": nz, "la": nlat, "lo": nlon})
                        clear_catalog_cache()
                        st.session_state.flash_msg = "✅ Zona agregada exitosamente."
                        st.rerun()
                    except:
                        st.toast("Error: Zona duplicada.", icon="❌")

        with t_equipos:
            st.markdown("#### Gestión de Equipos de Red")
            for eq in get_cat("cat_equipos"):
                c_en, c_edel = st.columns([5,1])
                c_en.text(f"🖥️ {eq}")
                if c_edel.button("🗑️ Eliminar", key=f"del_eq_{eq}"):
                    try:
                        with engine.begin() as conn:
                            conn.execute(text("DELETE FROM cat_equipos WHERE nombre=:n"), {"n": eq})
                        clear_catalog_cache()
                        st.session_state.flash_msg = "🗑️ Equipo eliminado."
                        st.rerun()
                    except Exception:
                        st.session_state.flash_msg  = "❌ Error: El equipo está en uso por un incidente."
                        st.session_state.flash_type = "error"
                        st.rerun()
            st.divider()
            with st.form("form_add_eq", clear_on_submit=True):
                st.markdown("**➕ Agregar Nuevo Equipo**")
                ne = st.text_input("Nombre del dispositivo/equipo")
                if st.form_submit_button("Agregar Equipo") and ne:
                    try:
                        with engine.begin() as conn:
                            conn.execute(text("INSERT INTO cat_equipos (nombre) VALUES (:n)"), {"n": ne})
                        clear_catalog_cache()
                        st.session_state.flash_msg = "✅ Equipo agregado exitosamente."
                        st.rerun()
                    except:
                        st.toast("Error: Equipo duplicado.", icon="❌")

        with t_causas:
            st.markdown("#### Gestión de Causas Raíz")
            for causa, ext in get_causas_con_flag().items():
                c_cn, c_ct, c_cdel = st.columns([4,2,1])
                c_cn.text(f"🛠️ {causa}")
                c_ct.caption("Externa (Fuerza Mayor)" if ext else "Interna (NOC)")
                if c_cdel.button("🗑️ Eliminar", key=f"del_ca_{causa}"):
                    try:
                        with engine.begin() as conn:
                            conn.execute(text("DELETE FROM cat_causas WHERE nombre=:n"), {"n": causa})
                        clear_catalog_cache()
                        st.session_state.flash_msg = "🗑️ Causa eliminada."
                        st.rerun()
                    except Exception:
                        st.session_state.flash_msg  = "❌ Error: La causa está en uso."
                        st.session_state.flash_type = "error"
                        st.rerun()
            st.divider()
            with st.form("form_add_causa", clear_on_submit=True):
                st.markdown("**➕ Agregar Nueva Causa**")
                nc  = st.text_input("Descripción de la causa")
                nce = st.checkbox("¿Es un factor Externo (Terceros, Clima, etc)?")
                if st.form_submit_button("Agregar Causa") and nc:
                    try:
                        with engine.begin() as conn:
                            conn.execute(text("INSERT INTO cat_causas (nombre,es_externa) VALUES (:n,:e)"), {"n": nc, "e": nce})
                        clear_catalog_cache()
                        st.session_state.flash_msg = "✅ Causa agregada exitosamente."
                        st.rerun()
                    except:
                        st.toast("Error: Causa duplicada.", icon="❌")

        with t_usuarios:
            st.markdown("#### Control de Accesos y Contraseñas")
            
            cu, clg = st.columns([1, 2], gap="large")
            with cu:
                with st.form("form_u", clear_on_submit=True):
                    st.markdown("**Crear Usuario**")
                    nu   = st.text_input("Usuario")
                    np_u = st.text_input("Contraseña", type="password")
                    st.caption("🔒 *Mínimo 8 caracteres, incluyendo 1 mayúscula, 1 minúscula, 1 número y 1 carácter especial (@$!%*?&)*")
                    nrl  = st.selectbox("Rol Asignado", ["viewer", "auditor", "admin"])
                    
                    if st.form_submit_button("Crear Cuenta") and nu and np_u:
                        if not es_password_segura(np_u):
                            st.session_state.flash_msg = "La contraseña no cumple con los requisitos de seguridad."
                            st.session_state.flash_type = "error"
                            st.rerun()
                        else:
                            try:
                                with engine.begin() as conn:
                                    conn.execute(
                                        text("INSERT INTO users (username,password_hash,role) VALUES (:u,:h,:r)"),
                                        {"u": nu, "h": hash_pw(np_u), "r": nrl}
                                    )
                                st.session_state.flash_msg = "✅ Usuario creado exitosamente."
                                st.rerun()
                            except Exception as e:
                                if "unique constraint" in str(e).lower() or "duplicate key" in str(e).lower():
                                    st.toast("❌ Error: Ese nombre de usuario ya existe.", icon="❌")
                                else:
                                    st.toast(f"❌ Error interno: {str(e)}", icon="❌")

                with st.form("form_reset_pw", clear_on_submit=True):
                    st.markdown("**Restablecer Contraseña**")
                    u_reset = st.text_input("Usuario exacto")
                    p_reset = st.text_input("Nueva Contraseña", type="password")
                    
                    if st.form_submit_button("Restablecer") and u_reset and p_reset:
                        if not es_password_segura(p_reset):
                            st.session_state.flash_msg = "La contraseña nueva no cumple con los requisitos de seguridad."
                            st.session_state.flash_type = "error"
                            st.rerun()
                        else:
                            try:
                                with engine.begin() as conn:
                                    res = conn.execute(
                                        text("UPDATE users SET password_hash=:h, failed_attempts=0, locked_until=NULL WHERE username=:u"),
                                        {"h": hash_pw(p_reset), "u": u_reset}
                                    )
                                    if res.rowcount > 0:
                                        st.session_state.flash_msg = "✅ Contraseña actualizada."
                                        st.rerun()
                                    else:
                                        st.toast("❌ Usuario no encontrado.", icon="❌")
                            except Exception as e:
                                st.toast(f"Error: {e}")

            with clg:
                try:
                    with engine.connect() as conn:
                        df_usrs = pd.read_sql(
                            text("SELECT id,username,role,is_banned,failed_attempts FROM users"), conn)
                    df_usrs.insert(0, "Sel", False)
                    
                    ed_usrs = st.data_editor(
                        df_usrs,
                        column_config={
                            "Sel":             st.column_config.CheckboxColumn("✔", default=False),
                            "id":              None,
                            "username":        "Usuario",
                            "role":            "Rol",
                            "is_banned":       "Baneado",
                            "failed_attempts": "Intentos Fallidos",
                        },
                        use_container_width=True, hide_index=True,
                        key="editor_usuarios_v10"
                    )
                    filas_del   = ed_usrs[ed_usrs["Sel"] == True]
                    hay_cambios = not (df_usrs.drop(columns=['Sel'], errors='ignore').reset_index(drop=True)
                                       .equals(ed_usrs.drop(columns=['Sel'], errors='ignore').reset_index(drop=True)))

                    u1c, u2c = st.columns(2)
                    if not filas_del.empty and u1c.button("🗑️ Eliminar Usuario", use_container_width=True):
                        if "Admin" in filas_del['username'].values:
                            st.error("❌ No se puede eliminar la cuenta Admin raíz.")
                        elif st.session_state.get('username') in filas_del['username'].values:
                            st.session_state.flash_msg  = "❌ No puedes eliminar tu propia cuenta mientras estás en sesión."
                            st.session_state.flash_type = "error"
                            st.rerun()
                        else:
                            with engine.begin() as conn:
                                for rid in filas_del['id']:
                                    conn.execute(text("DELETE FROM users WHERE id=:id"), {"id": int(rid)})
                            st.session_state.flash_msg = "🗑️ Usuario eliminado."
                            st.rerun()

                    if hay_cambios and u2c.button("💾 Guardar Permisos", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for i, er in ed_usrs.iterrows():
                                orig = df_usrs.drop(columns=['Sel'], errors='ignore').iloc[i]
                                if not orig.equals(er.drop('Sel', errors='ignore')):
                                    conn.execute(
                                        text("UPDATE users SET role=:r,is_banned=:b,failed_attempts=:f WHERE id=:id"),
                                        {"r": str(er.get('role', 'viewer')), "b": bool(er.get('is_banned', False)),
                                         "f": int(er.get('failed_attempts', 0)), "id": int(er.get('id'))}
                                    )
                        st.session_state.flash_msg = "💾 Permisos de usuario guardados."
                        st.rerun()
                except Exception as e:
                    st.error(str(e))

            st.divider()
            st.markdown("##### 📜 Registro del Sistema (Audit Log)")
            try:
                with engine.connect() as conn:
                    logs = pd.read_sql(
                        text("SELECT timestamp AS Fecha, username AS Usuario, action AS Accion, details AS Detalles FROM audit_logs ORDER BY id DESC"),
                        conn
                    )
                t_lp  = max(1, math.ceil(len(logs)/10))
                p_log = st.number_input("Página de log", 1, t_lp, 1)
                st.dataframe(logs.iloc[(p_log-1)*10 : p_log*10], use_container_width=True, hide_index=True)
            except Exception as e:
                st.warning(str(e))
