import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests, time, bcrypt, math, pytz, calendar, json
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
PALETA_CORP     = [COLOR_PRIMARY, COLOR_SECONDARY, COLOR_TEAL, '#ff9f43', '#83c9ff', COLOR_DANGER]

DEFAULT_ZONAS = [
    ("El Rosario", 13.4886, -89.0256), ("ARG", 13.4880, -89.3200), ("Tepezontes", 13.6214, -89.0125),
    ("La Libertad", 13.4883, -89.3200), ("El Tunco", 13.4930, -89.3830), ("Costa del Sol", 13.3039, -88.9450),
    ("Zacatecoluca", 13.5048, -88.8710), ("Zaragoza", 13.5850, -89.2890), ("Santiago Nonualco", 13.5186, -88.9442),
    ("Rio Mar", 13.4900, -89.3500), ("San Salvador (Central)", 13.6929, -89.2182),
]
DEFAULT_EQUIPOS = [
    "ONT", "Repetidor Wi-Fi", "Antena Ubiquiti", "OLT", "Caja NAP", "Switch", "Fibra Principal",
    "Servidor de Aplicativos", "Servidor DNS", "Servidor Virtual", "Mikrotik Concentrador", 
    "Encoder", "Caja Señal TV", "Antena Señal TV"
]
DEFAULT_CAUSAS = [
    ("Corte de Fibra por Terceros", True), ("Corte de Fibra (No Especificado)", False), ("Caída de Árboles sobre Fibra", True),
    ("Falla de Energía Comercial", True), ("Corrosión en Equipos", False), ("Daños por Fauna", True),
    ("Falla de Hardware", False), ("Falla de Configuración", False), ("Falla de Redundancia", False),
    ("Saturación de Tráfico", False), ("Saturación en Servidor", False), ("Mantenimiento Programado", False), 
    ("Vandalismo o Hurto", True), ("Condiciones Climáticas", True),
]
DEFAULT_SERVICIOS = ["Internet", "Cable TV (CATV)", "IPTV (Mnet+)", "Internet/Cable TV", "Aplicativos internos"]
DEFAULT_CATEGORIAS = ["Red Multinet", "Cliente Corporativo", "Falla Interna (No afecta clientes)"]
CAT_INTERNA = "Falla Interna (No afecta clientes)"
MESES = ["Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]

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

for _k, _v in [('form_reset', 0), ('logged_in', False), ('role', ''), ('username', ''), ('log_u', ''), ('log_p', ''), ('log_err', ''), ('log_msg', '')]:
    if _k not in st.session_state: st.session_state[_k] = _v

# =====================================================================
# BASE DE DATOS Y NUEVA ARQUITECTURA RELACIONAL
# =====================================================================
@st.cache_resource
def get_engine(): return create_engine(st.secrets["neon_dsn"], pool_pre_ping=True, pool_recycle=300)
engine = get_engine()

def hash_pw(p: str) -> str: return bcrypt.hashpw(p.encode(), bcrypt.gensalt()).decode()
def check_pw(p: str, h: str) -> bool: return bcrypt.checkpw(p.encode(), h.encode())

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
            for n, la, lo in DEFAULT_ZONAS: conn.execute(text("INSERT INTO cat_zonas (nombre,lat,lon) VALUES (:n,:lat,:lon) ON CONFLICT DO NOTHING"), {"n": n, "lat": la, "lon": lo})
        if conn.execute(text("SELECT count(*) FROM cat_equipos")).scalar() == 0:
            for e in DEFAULT_EQUIPOS: conn.execute(text("INSERT INTO cat_equipos (nombre) VALUES (:n) ON CONFLICT DO NOTHING"), {"n": e})
        if conn.execute(text("SELECT count(*) FROM cat_causas")).scalar() == 0:
            for n, ext in DEFAULT_CAUSAS: conn.execute(text("INSERT INTO cat_causas (nombre,es_externa) VALUES (:n,:e) ON CONFLICT DO NOTHING"), {"n": n, "e": ext})
        if conn.execute(text("SELECT count(*) FROM cat_servicios")).scalar() == 0:
            for s in DEFAULT_SERVICIOS: conn.execute(text("INSERT INTO cat_servicios (nombre) VALUES (:n) ON CONFLICT DO NOTHING"), {"n": s})
        
        if conn.execute(text("SELECT count(*) FROM users WHERE username='Admin'")).scalar() == 0:
            conn.execute(text("INSERT INTO users (username,password_hash,role) VALUES ('Admin',:h,'admin')"), {"h": hash_pw("Areakde5")})

try: init_db()
except Exception as _e: st.error(f"Error DB Inicialización: {_e}")

@st.cache_data(ttl=300, show_spinner=False)
def get_zonas() -> list[tuple]:
    try:
        with engine.connect() as c: return [(r[0], r[1], r[2]) for r in c.execute(text("SELECT nombre, lat, lon FROM cat_zonas ORDER BY nombre")).fetchall()]
    except: return list(DEFAULT_ZONAS)

@st.cache_data(ttl=300, show_spinner=False)
def get_cat(tabla: str) -> list[str]:
    try:
        with engine.connect() as c: return [r[0] for r in c.execute(text(f"SELECT nombre FROM {tabla} ORDER BY nombre")).fetchall()]
    except: return []

@st.cache_data(ttl=300, show_spinner=False)
def get_causas_con_flag() -> dict:
    try:
        with engine.connect() as c: return {r[0]: r[1] for r in c.execute(text("SELECT nombre, es_externa FROM cat_causas ORDER BY nombre")).fetchall()}
    except: return {}

def clear_catalog_cache():
    get_zonas.clear(); get_cat.clear(); get_causas_con_flag.clear(); fetch_and_sync_smartolt.clear()

# =====================================================================
# MOTOR DE SINCRONIZACIÓN SMARTOLT -> NEON
# =====================================================================
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_and_sync_smartolt() -> dict:
    try:
        base_url = st.secrets.get("smartolt_api_url", "").rstrip("/")
        api_key  = st.secrets.get("smartolt_api_key", "")
        if not base_url or not api_key: return {"_error": "Faltan credenciales en st.secrets."}

        headers = {"X-Token": api_key}
        res = requests.get(f"{base_url}/api/onu/get_all_onus_details", headers=headers, timeout=15)
        
        if res.status_code == 403: return {"_error": "Error HTTP 403: Acceso denegado. Posible bloqueo por Rate Limit o API Key incorrecta."}
        if res.status_code != 200: return {"_error": f"Error HTTP {res.status_code}."}
        
        try: data = res.json()
        except: return {"_error": "La API no devolvió JSON."}
        
        onus_list = []
        if isinstance(data, list): onus_list = data
        elif isinstance(data, dict):
            for key in ["response", "onus", "data", "onus_status"]:
                if key in data and isinstance(data[key], list):
                    onus_list = data[key]; break

        if not onus_list: return {"_error": "No se encontraron ONUs activas o la API respondió vacío."}
            
        resultado = {}
        for onu in onus_list:
            if isinstance(onu, dict):
                olt_name = str(onu.get("olt_name", onu.get("olt", "OLT_Desconocida"))).strip()
                resultado[olt_name] = resultado.get(olt_name, 0) + 1
        
        with engine.begin() as conn:
            for olt_name, total in resultado.items():
                conn.execute(text("""
                    INSERT INTO cmdb_nodos (zona, equipo, clientes, fuente, ultima_sincronizacion) 
                    VALUES (:z, 'OLT', :c, 'SmartOLT', CURRENT_TIMESTAMP)
                    ON CONFLICT (zona, equipo) DO UPDATE 
                    SET clientes = EXCLUDED.clientes, fuente = 'SmartOLT', ultima_sincronizacion = CURRENT_TIMESTAMP
                """), {"z": olt_name, "c": total})
                
        return resultado
    except Exception as ex: return {"_error": str(ex)}

def get_clientes_cmdb(zona: str, equipo: str) -> dict:
    if zona == "San Salvador (Central)": return {"clientes": 0, "fuente": "Datacenter/Core"}
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT clientes, fuente FROM cmdb_nodos WHERE zona ILIKE :z AND equipo = :e ORDER BY id DESC LIMIT 1"), {"z": f"%{zona}%", "e": equipo}).fetchone()
            if res: return {"clientes": res[0], "fuente": res[1]}
    except: pass
    return {"clientes": 0, "fuente": "Manual"}

def smartolt_status_badge() -> str:
    data = fetch_and_sync_smartolt()
    if "_error" in data: return f"🔴 {data['_error']}"
    if not data: return "🟡 SmartOLT: Sin datos"
    return f"🟢 API Live: {len(data)} OLTs · {sum(v for v in data.values()):,} ONTs"

# =====================================================================
# CARGA Y ENRIQUECIMIENTO (RENDIMIENTO)
# =====================================================================
@st.cache_data(ttl=60, show_spinner=False)
def load_data_rango(fecha_ini: date, fecha_fin: date, include_deleted: bool = False, zona_filtro: str = "Todas", serv_filtro: str = "Todos", seg_filtro: str = "Todos") -> pd.DataFrame:
    s_date = datetime.combine(fecha_ini, datetime_time(0, 0, 0))
    e_date = datetime.combine(fecha_fin, datetime_time(23, 59, 59))
    
    conds = ["((inicio_incidente >= :s AND inicio_incidente <= :e) OR (fin_incidente >= :s AND fin_incidente <= :e) OR (inicio_incidente <= :e AND estado = 'Abierto'))"]
    conds.append("deleted_at IS NULL" if not include_deleted else "deleted_at IS NOT NULL")
    if zona_filtro != "Todas": conds.append(f"zona = '{zona_filtro.replace(chr(39), chr(39)*2)}'")
    if serv_filtro != "Todos": conds.append(f"servicio = '{serv_filtro.replace(chr(39), chr(39)*2)}'")
    if seg_filtro  != "Todos": conds.append(f"categoria = '{seg_filtro.replace(chr(39), chr(39)*2)}'")
    
    q = "SELECT * FROM incidents WHERE " + " AND ".join(conds) + " ORDER BY inicio_incidente ASC"
    try:
        with engine.connect() as conn: return pd.read_sql(text(q), conn, params={"s": s_date, "e": e_date})
    except: return pd.DataFrame()

def enriquecer(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    
    df['inicio_incidente'] = pd.to_datetime(df['inicio_incidente'], utc=True).dt.tz_convert(SV_TZ)
    df['fin_incidente'] = pd.to_datetime(df['fin_incidente'], errors='coerce', utc=True)
    m_not_null = df['fin_incidente'].notnull()
    df.loc[m_not_null, 'fin_incidente'] = df.loc[m_not_null, 'fin_incidente'].dt.tz_convert(SV_TZ)

    df['duracion_horas']     = pd.to_numeric(df['duracion_horas'], errors='coerce').fillna(0.0)
    df['clientes_afectados'] = pd.to_numeric(df['clientes_afectados'], errors='coerce').fillna(0).astype(int)
    causas_ext_map = get_causas_con_flag()

    def _severidad(r) -> str:
        if r['estado'] == 'Abierto': return '🚨 CRÍTICA (Falla en Curso)'
        if r['categoria'] == CAT_INTERNA: return '🟢 P4 (Interna)'
        if r['duracion_horas'] >= 12 or r['clientes_afectados'] >= 1000: return '🔴 P1 (Crítica)'
        if r['duracion_horas'] >= 4  or r['clientes_afectados'] >= 300:  return '🟠 P2 (Alta)'
        return '🟡 P3 (Media)'

    df['Severidad']     = df.apply(_severidad, axis=1)
    df['es_externa']    = df['causa_raiz'].map(lambda x: causas_ext_map.get(x, False))
    df['zona_completa'] = df.apply(lambda r: f"{r['zona']} (General)" if r.get('afectacion_general', True) else f"{r['zona']} - {r['subzona']}", axis=1)
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
        "t1": {"fallas": 0, "mttr": 0.0, "acd": 0.0, "clientes": 0},
        "t2": {"fallas": 0, "mttr": 0.0},
        "t3": {"fallas": 0, "clientes_est": 0},
        "int": {"fallas": 0, "mttr": 0.0},
        "abiertos": 0
    }
    if df.empty: return base

    base["abiertos"] = len(df[df['estado'] == 'Abierto'])

    # Solo usamos tickets cerrados para cálculos de SLA y MTTR
    df_cerrados = df[df['estado'] == 'Cerrado']
    
    df_int = df_cerrados[df_cerrados['categoria'] == CAT_INTERNA]; df_ext = df_cerrados[df_cerrados['categoria'] != CAT_INTERNA]

    base["int"]["fallas"] = len(df_int)
    if not df_int.empty:
        iv = df_int[df_int['duracion_horas'] > 0]
        base["int"]["mttr"] = float(iv['duracion_horas'].mean()) if not iv.empty else 0.0

    if df_ext.empty: return base

    base["global"]["total_fallas"] = len(df_ext)
    base["global"]["p1"]           = int((df_ext['Severidad'] == '🔴 P1 (Crítica)').sum())

    df_t3 = df_ext[df_ext['conocimiento_tiempos'] != 'Total']
    base["t3"]["fallas"]       = len(df_t3)
    base["t3"]["clientes_est"] = int(df_t3['clientes_afectados'].sum())

    df_exact = df_ext[df_ext['conocimiento_tiempos'] == 'Total']
    if not df_exact.empty:
        base["global"]["db"] = float(df_exact['duracion_horas'].sum())
        base["global"]["mh"] = float(df_exact['duracion_horas'].max())

    df_t2 = df_exact[df_exact['clientes_afectados'] == 0]
    base["t2"]["fallas"] = len(df_t2)
    if not df_t2.empty: base["t2"]["mttr"] = float(df_t2['duracion_horas'].mean())

    df_t1 = df_exact[df_exact['clientes_afectados'] > 0]
    base["t1"]["fallas"] = len(df_t1)
    if not df_t1.empty:
        base["t1"]["mttr"]     = float(df_t1['duracion_horas'].mean())
        base["t1"]["clientes"] = int(df_t1['clientes_afectados'].sum())
        total_hc = (df_t1['duracion_horas'] * df_t1['clientes_afectados']).sum()
        base["t1"]["acd"] = float(total_hc / base["t1"]["clientes"]) if base["t1"]["clientes"] > 0 else 0.0

    if not df_exact.empty:
        s_cl  = df_exact['inicio_incidente'].clip(lower=rng_s)
        e_cl  = df_exact['fin_incidente'].clip(upper=rng_e)
        valid = (s_cl <= e_cl)
        ivs   = [[s, e] for s, e in zip(s_cl[valid], e_cl[valid])]
        t_down = sum((e - s).total_seconds() for s, e in _merge_intervals(ivs)) / 3600.0
    else: t_down = 0.0

    base["global"]["sla"]  = max(0.0, min(100.0, (h_tot - t_down) / h_tot * 100)) if h_tot > 0 else 100.0
    base["global"]["mtbf"] = float((h_tot - t_down) / len(df_exact)) if len(df_exact) > 0 else float(h_tot)
    return base

# =====================================================================
# AUDITORÍA Y REPORTES
# =====================================================================
def log_audit(action: str, detail: str):
    try:
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO audit_logs (timestamp,username,action,details) VALUES (:t,:u,:a,:d)"), {"t": datetime.now(SV_TZ).replace(tzinfo=None), "u": st.session_state.get("username", "?"), "a": action, "d": detail})
    except: pass

def generar_pdf(label_periodo: str, kpis: dict, df: pd.DataFrame) -> bytes:
    def mk_style(name, size, font='Helvetica', color=rl_colors.black, **kw): return ParagraphStyle(name, fontSize=size, fontName=font, textColor=color, **kw)
    s_title  = mk_style('t',  22, 'Helvetica-Bold', rl_colors.HexColor(COLOR_SECONDARY),  spaceAfter=2)
    s_sub    = mk_style('s',  11, 'Helvetica',      rl_colors.HexColor('#888888'),  spaceAfter=14)
    s_period = mk_style('pe', 11, 'Helvetica-Bold', rl_colors.HexColor(COLOR_PRIMARY),spaceAfter=2)
    s_body   = mk_style('b',   9, 'Helvetica',      rl_colors.black, spaceAfter=4)
    s_sec    = mk_style('h',  13, 'Helvetica-Bold', rl_colors.HexColor(COLOR_SECONDARY),  spaceBefore=14, spaceAfter=6)

    def tbl_style(hdr_color):
        return TableStyle([('BACKGROUND',(0,0),(-1,0),hdr_color),('TEXTCOLOR',(0,0),(-1,0),rl_colors.white),('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),('FONTSIZE',(0,0),(-1,0),9),('ROWBACKGROUNDS',(0,1),(-1,-1),[rl_colors.HexColor('#f5f5f5'), rl_colors.white]),('FONTNAME',(0,1),(-1,-1),'Helvetica'),('FONTSIZE',(0,1),(-1,-1),9),('VALIGN',(0,0),(-1,-1),'MIDDLE'),('GRID',(0,0),(-1,-1),0.4, rl_colors.HexColor('#dddddd')),('ROWHEIGHT',(0,0),(-1,-1),20),('LEFTPADDING',(0,0),(-1,-1),7),('RIGHTPADDING',(0,0),(-1,-1),7),('TEXTCOLOR',(1,1),(1,-1),rl_colors.HexColor(COLOR_PRIMARY)),('FONTNAME',(1,1),(1,-1),'Helvetica-Bold'),('ALIGN',(1,0),(1,-1),'CENTER')])

    buffer  = BytesIO(); doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm); story = []; now_str = datetime.now(SV_TZ).strftime('%d/%m/%Y %H:%M')

    story += [Paragraph("MULTINET", s_title), Paragraph("Reporte Ejecutivo · Network Operations Center", s_sub), HRFlowable(width="100%", thickness=2, color=rl_colors.HexColor(COLOR_PRIMARY), spaceAfter=10), Paragraph(f"<b>Periodo:</b> {label_periodo}", s_period), Paragraph(f"<b>Generado:</b> {now_str} (hora El Salvador)", s_body), Spacer(1, 0.5*cm)]

    story.append(Paragraph("1. Métricas Operativas Principales", s_sec))
    kpi_rows = [
        ['Indicador', 'Valor', 'Descripción'],
        ['SLA Global (Disponibilidad)',   f"{kpis['global']['sla']:.3f}%",   'Intervalos fusionados (Solo tickets cerrados)'],
        ['MTTR Real',                     f"{kpis['t1']['mttr']:.2f} horas",'Resolución promedio (clientes > 0)'],
        ['ACD Real (Afectación)',         f"{kpis['t1']['acd']:.2f} horas", 'Percepción real del usuario'],
        ['Impacto Acumulado',             f"{(kpis['global']['db']/24):.2f} días",'Horas totales caídas / 24'],
        ['Clientes Impactados',           f"{kpis['t1']['clientes']:,}", 'Usuarios afectados (T1)'],
        ['MTBF (Estabilidad)',            f"{kpis['global']['mtbf']:.1f} horas",'Tiempo medio entre fallas'],
        ['Fallas P1 Críticas',            f"{kpis['global']['p1']}",          '>12 h o >1000 clientes'],
    ]
    t = Table(kpi_rows, colWidths=[5.5*cm, 3*cm, 8.5*cm]); t.setStyle(tbl_style(rl_colors.HexColor(COLOR_SECONDARY))); story += [t, Spacer(1, 0.4*cm)]

    story.append(Paragraph("2. Salud de Datos / Pendientes", s_sec))
    pend_rows = [['Estado', 'Cantidad'], ['Fallas Abiertas (En Curso)', str(kpis['abiertos'])], ['Fallas Incompletas (Sin hora exacta)', str(kpis['t3']['fallas'])], ['Incidentes internos', str(kpis['int']['fallas'])]]
    tp = Table(pend_rows, colWidths=[10*cm, 4*cm]); tp.setStyle(tbl_style(rl_colors.HexColor(COLOR_TEAL))); story += [tp, Spacer(1, 0.4*cm)]

    if not df.empty:
        df_ext = df[(df['categoria'] != CAT_INTERNA) & (df['estado'] == 'Cerrado')]
        if not df_ext.empty:
            story.append(Paragraph("3. Zonas con Mayor Afectación", s_sec)); top_z = df_ext.groupby('zona_completa')['duracion_horas'].sum().nlargest(8).reset_index()
            z_rows = [['Zona / Nodo', 'Horas', 'Días Equiv.']]
            for _, r in top_z.iterrows(): z_rows.append([str(r['zona_completa']), f"{r['duracion_horas']:.1f} h", f"{r['duracion_horas']/24:.2f}"])
            tz = Table(z_rows, colWidths=[9*cm, 4*cm, 4*cm]); tz.setStyle(tbl_style(rl_colors.HexColor(COLOR_TEAL))); story += [tz, Spacer(1, 0.4*cm)]

    story += [Spacer(1, 1*cm), HRFlowable(width="100%", thickness=0.5, color=rl_colors.HexColor('#dddddd'), spaceAfter=6), Paragraph(f"MULTINET NOC  ·  {now_str}  ·  Documento Confidencial", mk_style('f', 7, 'Helvetica', rl_colors.HexColor('#888888')))]
    doc.build(story); buffer.seek(0)
    return buffer.getvalue()

# =====================================================================
# LOGIN SECURE 
# =====================================================================
def do_login():
    u, p = st.session_state.log_u, st.session_state.log_p
    try:
        with engine.begin() as conn:
            ud = conn.execute(text("SELECT id,password_hash,role,failed_attempts,locked_until,is_banned FROM users WHERE username=:u"), {"u": u}).fetchone()
            if ud:
                uid, ph, rol, fa, ldt, ban = ud
                fa = fa or 0; now_sv = datetime.now(SV_TZ).replace(tzinfo=None)
                if ban: st.session_state.log_err = "❌ Cuenta baneada."
                elif ldt and ldt > now_sv: st.session_state.log_err = f"⏳ Bloqueado {(ldt - now_sv).seconds // 60 + 1} min."
                elif check_pw(p, ph):
                    conn.execute(text("UPDATE users SET failed_attempts=0,locked_until=NULL WHERE id=:id"), {"id": uid})
                    st.session_state.update({"logged_in": True, "role": rol, "username": u, "log_err": ""}); return
                else:
                    fa += 1
                    if fa >= 6: conn.execute(text("UPDATE users SET is_banned=TRUE,failed_attempts=:f WHERE id=:id"), {"f": fa,"id": uid}); st.session_state.log_err = "❌ Cuenta bloqueada permanente."
                    elif fa % 3 == 0: conn.execute(text("UPDATE users SET locked_until=:dt,failed_attempts=:f WHERE id=:id"), {"dt": now_sv + timedelta(minutes=5), "f": fa, "id": uid}); st.session_state.log_err = "⏳ Bloqueado 5 min."
                    else: conn.execute(text("UPDATE users SET failed_attempts=:f WHERE id=:id"), {"f": fa, "id": uid}); st.session_state.log_err = f"❌ Incorrecto. Intento {fa}/6."
            else: st.session_state.log_err = "❌ Usuario incorrecto."
    except Exception as ex: st.session_state.log_err = f"Error de conexión DB."
    st.session_state.log_u = ""; st.session_state.log_p = ""

if not st.session_state.logged_in:
    st.markdown("<div style='margin-top:15vh;'></div>", unsafe_allow_html=True)
    _, col_c, _ = st.columns([1, 1.2, 1])
    with col_c:
        if st.session_state.log_msg: st.toast(st.session_state.log_msg, icon="✅"); st.session_state.log_msg = ""
        if st.session_state.log_err: st.error(st.session_state.log_err); st.session_state.log_err = ""
        with st.container(border=True):
            st.markdown("<div style='text-align:center;padding:20px 0 10px 0;'><div style='font-size:46px;'>🔐</div><h2 style='margin:10px 0 4px;color:#fff;font-weight:700;'>Acceso NOC Central</h2></div>", unsafe_allow_html=True)
            st.text_input("Usuario", key="log_u"); st.text_input("Contraseña", key="log_p", type="password")
            st.button("Iniciar Sesión", type="primary", on_click=do_login, use_container_width=True)
            st.caption("Contacte al Administrador de Redes para gestionar sus credenciales.")
    st.stop()

# =====================================================================
# SIDEBAR
# =====================================================================
with st.sidebar:
    st.caption(f"👤 **{st.session_state.username}** ({st.session_state.role.capitalize()})  |  NOC v26.1")
    st.caption(smartolt_status_badge())
    st.divider()

    anio_act = datetime.now(SV_TZ).year
    a_sel = st.selectbox("🗓️ Año", sorted({anio_act+1, anio_act, anio_act-1, anio_act-2}, reverse=True), index=0)
    m_sel = st.selectbox("📅 Mes", MESES, index=datetime.now(SV_TZ).month - 1)
    m_idx = MESES.index(m_sel) + 1
    
    fecha_ini = date(a_sel, m_idx, 1)
    fecha_fin = date(a_sel, m_idx, calendar.monthrange(a_sel, m_idx)[1])
    label_periodo = f"{m_sel} {a_sel}"

    st.divider()
    z_sel   = st.selectbox("🗺️ Zona", ["Todas"] + [z[0] for z in get_zonas()])
    srv_sel = st.selectbox("🌐 Servicio", ["Todos"] + get_cat("cat_servicios"))
    seg_sel = st.selectbox("🏢 Segmento", ["Todos"] + DEFAULT_CATEGORIAS)

    df_m = enriquecer(load_data_rango(fecha_ini, fecha_fin, False, z_sel, srv_sel, seg_sel))

    p_m_idx = 12 if m_idx == 1 else m_idx - 1
    p_a_sel = a_sel - 1 if m_idx == 1 else a_sel
    p_fi    = date(p_a_sel, p_m_idx, 1)
    p_ff    = date(p_a_sel, p_m_idx, calendar.monthrange(p_a_sel, p_m_idx)[1])
    df_prev = enriquecer(load_data_rango(p_fi, p_ff, False, z_sel, srv_sel, seg_sel))

    st.divider()
    if not df_m.empty:
        st.download_button("📥 Descargar Reporte PDF", data=generar_pdf(label_periodo, calc_kpis(df_m, fecha_ini, fecha_fin), df_m), file_name=f"Reporte_NOC_{m_sel}_{a_sel}.pdf", mime="application/pdf", use_container_width=True)

    st.divider()
    if st.button("🚪 Cerrar Sesión", use_container_width=True): log_audit("LOGOUT", "Sesión cerrada."); st.session_state.clear(); st.rerun()

# =====================================================================
# PESTAÑAS POR ROL
# =====================================================================
role = st.session_state.role
if   role == 'admin':   tab_labels = ["📊 Dashboard", "📝 Registrar Evento", "🗂️ Historial y Edición", "⚙️ Configuración"]
elif role == 'auditor': tab_labels = ["📊 Dashboard", "📝 Registrar Evento", "🗂️ Historial y Edición"]
else:                   tab_labels = ["📊 Dashboard"]
tabs = st.tabs(tab_labels)

# ─────────────────────────────────────────────
# TAB 0 — DASHBOARD
# ─────────────────────────────────────────────
with tabs[0]:
    st.title(f"📊 Rendimiento de Red: {label_periodo}")

    # Corregido: En lugar de usar st.stop() y cortar el script, validamos si hay datos 
    # y solo dibujamos las métricas si df_m no está vacío.
    if df_m.empty: 
        st.success("🟢 Sin incidentes registrados en los filtros seleccionados. ¡La red está al 100%!")
    else:
        kpis      = calc_kpis(df_m, fecha_ini, fecha_fin)
        kpis_prev = calc_kpis(df_prev, p_fi, p_ff) if not df_prev.empty else None

        def _delta(cat, key, divisor=1, fmt="{:+.2f}", suffix=""):
            if not kpis_prev: return None
            return f"{fmt.format((kpis[cat][key] - kpis_prev[cat][key]) / divisor)}{' '+suffix if suffix else ''}"

        # ── 1. FALLAS EN CURSO (TICKETS ABIERTOS) ──
        df_abiertos = df_m[df_m['estado'] == 'Abierto']
        if not df_abiertos.empty:
            st.error(f"🚨 Tienes {len(df_abiertos)} Falla(s) en Curso (Tickets Abiertos).", icon="🚨")
            st.dataframe(df_abiertos[['zona_completa', 'equipo_afectado', 'inicio_incidente', 'causa_raiz', 'descripcion']].rename(columns={'zona_completa':'Nodo', 'equipo_afectado':'Equipo', 'inicio_incidente':'Hora de Caída', 'causa_raiz':'Diagnóstico'}), hide_index=True, use_container_width=True)
            st.divider()

        # ── 2. ESTADO GENERAL ──
        st.markdown("### 📈 Estado General de la Red")
        st.caption("*Métricas globales calculadas con registros cerrados que poseen duración exacta.*")

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("SLA Global", f"{kpis['global']['sla']:.3f}%", delta=_delta('global','sla', fmt="{:+.3f}", suffix="%"), delta_color="normal", help="Disponibilidad total. Intervalos solapados fusionados — sin doble descuento.")
        c2.metric("Total Eventos Registrados", kpis['global']['total_fallas'], delta=_delta('global','total_fallas', fmt="{:+.0f}"), delta_color="inverse", help="Suma de todas las fallas externas registradas.")
        c3.metric("MTBF (Estabilidad)", f"{kpis['global']['mtbf']:.1f} horas", delta=_delta('global','mtbf', fmt="{:+.1f}", suffix="h"), delta_color="normal", help="Tiempo Medio Entre Fallas — con datos exactos.")
        c4.metric("Impacto Acumulado", f"{kpis['global']['db']/24:.2f} días", delta=_delta('global','db', divisor=24, fmt="{:+.2f}", suffix="d"), delta_color="inverse", help="Total de horas de caída / 24.")
        c5.metric("Falla Mayor (Pico)", f"{kpis['global']['mh']:.1f} horas", delta=_delta('global','mh', fmt="{:+.1f}", suffix="h"), delta_color="inverse", help="Duración del evento más largo del periodo.")

        st.divider()
        # ── 3. IMPACTO A CLIENTES ──
        st.markdown("### 👥 Impacto Directo a Clientes (Datos Exactos)")
        st.caption("*Solo contempla eventos con duración exacta y clientes afectados > 0.*")

        if kpis['t1']['fallas'] == 0: st.info("ℹ️ No hay incidentes masivos con tiempos exactos en este periodo.")
        else:
            t1a, t1b, t1c, t1d = st.columns(4)
            t1a.metric("Fallas Completas", kpis['t1']['fallas'], delta=_delta('t1','fallas', fmt="{:+.0f}"), delta_color="inverse", help="Eventos que poseen duración exacta y número real de clientes > 0.")
            t1b.metric("MTTR Real (Resolución)", f"{kpis['t1']['mttr']:.2f} horas", delta=_delta('t1','mttr', fmt="{:+.2f}", suffix="h"), delta_color="inverse", help="Tiempo Promedio de Resolución. Tiempo de arreglo de fallas masivas.")
            t1c.metric("ACD Real (Afectación)", f"{kpis['t1']['acd']:.2f} horas", delta=_delta('t1','acd', fmt="{:+.2f}", suffix="h"), delta_color="inverse", help="Afectación Promedio por Cliente. Sensación real de corte por usuario.")
            t1d.metric("Clientes Afectados", f"{kpis['t1']['clientes']:,}", delta=_delta('t1','clientes', fmt="{:+.0f}"), delta_color="inverse", help="Suma total de clientes que sufrieron cortes en este mes.")

        st.divider()
        # ── 4. SALUD DE DATOS ──
        st.markdown("### ⚠️ Salud de Datos y Eventos Aislados")
        col_t2, col_t3, col_int = st.columns(3)

        for container, label, emoji_on, emoji_off, metric_a, val_a, help_a, metric_b, val_b, help_b in [
            (col_t2, "Fallas de Red Pura", "🟡","🟢", "Eventos sin Clientes", kpis['t2']['fallas'], "Hora exacta, 0 clientes. Sí afectan SLA.", "MTTR Infraestructura", f"{kpis['t2']['mttr']:.2f} horas", "Resolución de fallas de red aisladas."),
            (col_t3, "Eventos Incompletos", "🔴","🟢", "Fallas Sin Tiempo", kpis['t3']['fallas'], "Sin hora de inicio o cierre. Excluidas del SLA.", "Clientes Estimados", f"{kpis['t3']['clientes_est']:,}", "Posibles afectados sin duración medible."),
            (col_int, "Internas / Mantenimiento", "🔵","🟢", "Eventos Internos", kpis['int']['fallas'], "No impactan SLA externo.", "MTTR Interno", f"{kpis['int']['mttr']:.2f} horas", "Tiempo de resolución interno."),
        ]:
            v = val_a if isinstance(val_a, int) else kpis['t3']['fallas']
            with container:
                with st.container(border=True):
                    st.markdown(f"#### {emoji_on if v > 0 else emoji_off} {label}")
                    st.metric(metric_a, val_a, help=help_a); st.metric(metric_b, val_b, help=help_b)

        st.divider()
        
        # ── 5. GRÁFICOS ──
        st.markdown("### 🗺️ Análisis Geográfico y Temporal")
        zonas_coords = {z[0]: (z[1], z[2]) for z in get_zonas()}
        df_map = df_m.copy()
        df_map['lat'] = df_map['zona'].map(lambda x: zonas_coords.get(x, (13.6929, -89.2182))[0])
        df_map['lon'] = df_map['zona'].map(lambda x: zonas_coords.get(x, (13.6929, -89.2182))[1])
        agg = (df_map.groupby(['zona_completa','lat','lon']).agg(Horas=('duracion_horas','sum'), Clientes=('clientes_afectados','sum')).reset_index())
        agg['Clientes_sz'] = agg['Clientes'].clip(lower=1)
        
        fig_map = px.scatter_mapbox(agg, lat="lat", lon="lon", hover_name="zona_completa", size="Clientes_sz", color="Horas", color_continuous_scale="Inferno", zoom=8.5, mapbox_style="carto-darkmatter", labels={"Clientes_sz": "Clientes"}, title="Impacto Geográfico por Nodo")
        fig_map.update_layout(margin=dict(l=0,r=0,t=40,b=0), height=450)
        st.plotly_chart(fig_map, use_container_width=True)

        st.write("")
        dias_map   = {0:'Lunes',1:'Martes',2:'Miércoles',3:'Jueves',4:'Viernes',5:'Sábado',6:'Domingo'}
        dias_orden = list(dias_map.values())
        df_heat = df_m.copy()
        df_heat['Día']  = pd.Categorical(df_heat['inicio_incidente'].dt.dayofweek.map(dias_map), categories=dias_orden, ordered=True)
        df_heat['Hora'] = df_heat['inicio_incidente'].dt.hour
        df_hm = df_heat.groupby(['Día','Hora']).size().reset_index(name='Fallas')
        
        fig_hm = px.density_heatmap(df_hm, x='Hora', y='Día', z='Fallas', color_continuous_scale='Blues', nbinsx=24, title="Mapa de Calor (Concentración de Fallas por Hora)")
        fig_hm.update_layout(xaxis=dict(tickmode='linear', tick0=0, dtick=1), margin=dict(l=0,r=0,t=40,b=0), paper_bgcolor="rgba(0,0,0,0)", height=350)
        st.plotly_chart(fig_hm, use_container_width=True)

        st.write("")
        st.divider()
        st.markdown("### 📊 Responsabilidad y Causas Principales")
        
        c_pie, c_bar = st.columns(2)
        with c_pie:
            df_m['Tipo'] = df_m['es_externa'].map({True: 'Externa (Fuerza Mayor)', False: 'Interna (Infraestructura / NOC)'})
            agg_r = df_m.groupby('Tipo').size().reset_index(name='Eventos')
            fig_p = px.pie(agg_r, names='Tipo', values='Eventos', hole=0.5, color_discrete_sequence=[COLOR_TEAL, COLOR_DANGER], title="Tasa de Responsabilidad")
            fig_p.update_traces(textinfo='percent', textposition='inside')
            fig_p.update_layout(showlegend=True, legend=dict(orientation="h", yanchor="top", y=-0.1, xanchor="center", x=0.5), margin=dict(l=0,r=0,t=40,b=0), paper_bgcolor="rgba(0,0,0,0)", height=400)
            st.plotly_chart(fig_p, use_container_width=True)
            
        with c_bar:
            dc = (df_m.groupby('causa_raiz').size().reset_index(name='Alertas').sort_values('Alertas', ascending=True).tail(8))
            fig_b = px.bar(dc, x='Alertas', y='causa_raiz', orientation='h', color_discrete_sequence=[COLOR_PRIMARY], text_auto='.0f', title="Top Causas Raíz")
            fig_b.update_traces(textposition='outside')
            fig_b.update_layout(margin=dict(l=0,r=0,t=40,b=0), paper_bgcolor="rgba(0,0,0,0)", height=400, xaxis_title="", yaxis_title="")
            st.plotly_chart(fig_b, use_container_width=True)

# ─────────────────────────────────────────────
# TAB 1 — REGISTRAR EVENTO
# ─────────────────────────────────────────────
if role in ('admin', 'auditor') and len(tabs) > 1:
    with tabs[1]:
        st.title("📝 Registrar Evento Operativo")
        fk = st.session_state.form_reset

        zonas_form   = [z[0] for z in get_zonas()]
        equipos_form = get_cat("cat_equipos")
        causas_form  = list(get_causas_con_flag().keys())
        servs_form   = get_cat("cat_servicios")

        cf, ccx = st.columns([2, 1], gap="large")
        with cf:
            with st.container(border=True):
                c_z1, c_z2 = st.columns([1,1])
                with c_z1: z_f = st.selectbox("📍 Nodo Principal", zonas_form, key=f"z_{fk}")
                with c_z2:
                    st.markdown("<div style='margin-top:32px;'></div>", unsafe_allow_html=True)
                    ag  = st.toggle("🚨 Falla General (Afecta todo el nodo)", value=True, key=f"ag_{fk}")

                sz = st.text_input("📍 Sub-zona (Ej. Colonia Escalón)", value="General" if ag else "", key=f"sz_{fk}", disabled=ag)
                sz_db = "General" if ag else sz
                st.divider()

                c1f, c2f = st.columns(2)
                srv_f = c1f.selectbox("🌐 Servicio",  servs_form,      key=f"s_{fk}")
                cat_f = c2f.selectbox("🏢 Segmento",  DEFAULT_CATEGORIAS, key=f"c_{fk}")
                eq_f  = st.selectbox("🖥️ Equipo",     equipos_form,    key=f"e_{fk}")

                cmdb_data = get_clientes_cmdb(z_f, eq_f)
                d_cl = cmdb_data['clientes']
                fuente_dato = cmdb_data['fuente']

                st.divider()
                
                # Reglas estrictas de clientes
                if z_f == "San Salvador (Central)":
                    cl_f = 0
                    imp_pct = 0.0
                    st.number_input("👤 Clientes Afectados", value=0, disabled=True, key=f"cl_{fk}")
                    st.warning("⚠️ Centro de Datos: Equipos Core y Broadcast. No aplica conteo directo de ONTs.")
                elif cat_f == "Cliente Corporativo":
                    cl_f = 1
                    imp_pct = 0.0
                    st.number_input("👤 Clientes Afectados", value=1, disabled=True, key=f"cl_{fk}")
                    st.caption("🏢 Corporativo: fijado en 1 enlace.")
                elif cat_f == CAT_INTERNA:
                    cl_f = 0
                    imp_pct = 0.0
                    st.number_input("👤 Clientes Afectados", value=0, disabled=True, key=f"cl_{fk}")
                    st.caption("🔧 Interna: fijado en 0 clientes.")
                else:
                    if fuente_dato == 'SmartOLT': st.info(f"🔴 **SmartOLT:** {d_cl:,} clientes totales aprovisionados en esta OLT")
                    cl_f = st.number_input(f"👤 Clientes Afectados *(fuente base: {fuente_dato})*", min_value=0, value=d_cl, step=1, key=f"cl_{fk}")
                    imp_pct = round((cl_f / d_cl) * 100, 2) if d_cl > 0 else 0.0

                st.divider()
                
                # TICKETS ABIERTOS / CERRADOS
                estado_ticket = st.radio("🚦 Estado del Evento", ["Cerrado (Falla Resuelta)", "Abierto (Falla en Curso)"], horizontal=True, key=f"estado_{fk}")
                es_abierto = "Abierto" in estado_ticket
                
                ct1, ct2 = st.columns(2)
                with ct1:
                    fi  = st.date_input("📅 Fecha de Inicio", key=f"fi_{fk}")
                    hi_desc = st.checkbox("No conozco la hora de inicio", key=f"hi_desc_{fk}")
                    if not hi_desc: hi_val = st.time_input("🕒 Hora de Inicio", key=f"hi_val_{fk}")
                    else: hi_val = None; st.info("ℹ️ Sin hora de inicio → Evento Incompleto.")
                with ct2:
                    if es_abierto:
                        ff = fi
                        hf_val = None
                        hf_desc = True
                        st.warning("🚨 El ticket quedará Abierto. No requiere hora de cierre.")
                    else:
                        ff  = st.date_input("📅 Fecha de Cierre", key=f"ff_{fk}")
                        hf_desc = st.checkbox("No conozco la hora de cierre", key=f"hf_desc_{fk}")
                        if not hf_desc: hf_val = st.time_input("🕒 Hora de Cierre", key=f"hf_val_{fk}")
                        else: hf_val = None; st.info("ℹ️ Sin hora de cierre → Evento Incompleto.")

                dur = 0.0
                conocimiento = "Parcial"
                if (not hi_desc) and (not hf_desc) and (not es_abierto):
                    dt_ini = datetime.combine(fi, hi_val)
                    dt_fin = datetime.combine(ff, hf_val)
                    if dt_fin > dt_ini:
                        dur = round((dt_fin - dt_ini).total_seconds() / 3600, 2)
                        conocimiento = "Total"

                st.write("")
                cr_f   = st.selectbox("🛠️ Causa Raíz", causas_form, key=f"cr_{fk}")
                desc_f = st.text_area("📝 Descripción del Evento", key=f"desc_{fk}")

                if st.button("💾 Guardar Registro", type="primary"):
                    err = False
                    if (not es_abierto) and (not hi_desc) and (not hf_desc) and (fi > ff or (fi == ff and hi_val >= hf_val)):
                        st.toast("❌ Error: el cierre no puede ser anterior al inicio.", icon="🚨")
                        err = True

                    if not err:
                        with st.spinner("Guardando en Base de Datos…"):
                            try:
                                hi_db = hi_val if not hi_desc else datetime_time(0, 0)
                                idi = SV_TZ.localize(datetime.combine(fi, hi_db))
                                
                                if es_abierto or hf_desc: idf = None
                                else: idf = SV_TZ.localize(datetime.combine(ff, hf_val))
                                
                                estado_db = 'Abierto' if es_abierto else 'Cerrado'

                                with engine.begin() as conn:
                                    conn.execute(text("""
                                        INSERT INTO incidents (zona, subzona, afectacion_general, servicio, categoria, equipo_afectado, estado, inicio_incidente, fin_incidente, clientes_afectados, impacto_porcentaje, causa_raiz, descripcion, duracion_horas, conocimiento_tiempos)
                                        VALUES (:z, :sz, :ag, :s, :c, :e, :est, :idi, :idf, :cl, :imp, :cr, :d, :dur, :con)
                                    """), {"z": z_f, "sz": sz_db, "ag": ag, "s": srv_f, "c": cat_f, "e": eq_f, "est": estado_db, "idi": idi, "idf": idf, "cl": cl_f, "imp": imp_pct, "cr": cr_f, "d": desc_f, "dur": dur, "con": conocimiento})

                                    if z_f != "San Salvador (Central)":
                                        conn.execute(text("""
                                            INSERT INTO cmdb_nodos (zona, equipo, clientes, fuente) VALUES (:z, :e, :cl, 'Manual')
                                            ON CONFLICT (zona, equipo) DO UPDATE SET clientes = EXCLUDED.clientes WHERE EXCLUDED.clientes > cmdb_nodos.clientes
                                        """), {"z": z_f, "e": eq_f, "cl": cl_f})

                                log_audit("INSERT", f"Falla ({estado_db}) en {z_f} | {eq_f}")
                                load_data_rango.clear()
                                st.session_state.form_reset += 1
                                st.rerun()
                            except Exception as e: st.toast(f"Error al guardar: {e}", icon="❌")

        with ccx:
            st.markdown("#### 🕒 Registros Recientes")
            if not df_m.empty:
                for _, r in df_m.sort_values('id', ascending=False).head(6).iterrows():
                    with st.container(border=True):
                        ico = "🚨" if r.get('estado') == 'Abierto' else ("🔧" if r.get('categoria') == CAT_INTERNA else "📡")
                        st.markdown(f"**{ico} {r['zona_completa']}**")
                        st.caption(f"{str(r.get('causa_raiz',''))[:30]}… | 👥 {r['clientes_afectados']}")
            else: st.info("Sin registros en este periodo.")

# ─────────────────────────────────────────────
# TAB 2 — HISTORIAL Y EDICIÓN
# ─────────────────────────────────────────────
if role in ('admin', 'auditor') and len(tabs) > 2:
    with tabs[2]:
        st.markdown("### 🗂️ Historial, Auditoría y Edición")
        papelera = st.toggle("🗑️ Explorar Papelera de Reciclaje")
        df_audit = enriquecer(load_data_rango(fecha_ini, fecha_fin, papelera, "Todas", "Todos", "Todos"))

        if df_audit.empty: st.info("No hay datos en el servidor para el periodo seleccionado.")
        else:
            c_s, c_pg = st.columns([4, 1])
            bq = c_s.text_input("🔎 Buscar:", placeholder="Causa, nodo, equipo…")
            df_d = (df_audit[df_audit.astype(str).apply(lambda x: x.str.contains(bq, case=False, na=False)).any(axis=1)].copy() if bq else df_audit.copy())

            tot_p = max(1, math.ceil(len(df_d)/15))
            pg = c_pg.number_input("Página", 1, tot_p, 1, key="p_bd")
            df_page = df_d.iloc[(pg-1)*15 : pg*15].copy()
            df_page.insert(0, "Sel", False)

            drop_cols = [c for c in ['deleted_at','Severidad','zona_completa','es_externa', 'impacto_porcentaje'] if c in df_page.columns]

            ed_df = st.data_editor(
                df_page.drop(columns=drop_cols, errors='ignore'),
                column_config={
                    "Sel": st.column_config.CheckboxColumn("✔", default=False), 
                    "id": None, 
                    "estado": st.column_config.SelectboxColumn("Estado", options=["Cerrado", "Abierto"]),
                    "inicio_incidente": st.column_config.DatetimeColumn("Inicio", format="YYYY-MM-DD HH:mm"), 
                    "fin_incidente": st.column_config.DatetimeColumn("Fin", format="YYYY-MM-DD HH:mm")
                },
                use_container_width=True, hide_index=True,
            )

            f_sel = ed_df[ed_df["Sel"] == True]
            ref_df = (df_page.drop(columns=drop_cols + ['Sel'], errors='ignore').reset_index(drop=True))

            def strip_tz(s):
                if pd.api.types.is_datetime64_any_dtype(s): return s.dt.tz_convert(None) if (hasattr(s.dt,'tz') and s.dt.tz) else s
                return s

            ed_cmp = ed_df.drop(columns=['Sel']).copy().apply(strip_tz)
            ref_cmp = ref_df.copy().apply(strip_tz)
            h_cam = not ref_cmp.equals(ed_cmp)

            cb1, cb2 = st.columns(2)

            if not f_sel.empty:
                if papelera:
                    if cb1.button("♻️ Restaurar Seleccionados", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for rid in f_sel['id']: conn.execute(text("UPDATE incidents SET deleted_at=NULL WHERE id=:id"), {"id": int(rid)})
                        log_audit("RESTORE", f"{len(f_sel)} registro(s)."); load_data_rango.clear(); st.rerun()
                else:
                    if cb1.button("🗑️ Eliminar Seleccionados", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for rid in f_sel['id']: conn.execute(text("UPDATE incidents SET deleted_at=CURRENT_TIMESTAMP WHERE id=:id"), {"id": int(rid)})
                        log_audit("DELETE (SOFT)", f"{len(f_sel)} registro(s)."); load_data_rango.clear(); st.rerun()

            if h_cam and cb2.button("💾 Guardar Cambios (Cerrar Tickets)", type="primary", use_container_width=True):
                with engine.begin() as conn:
                    for i, r in ed_df.iterrows():
                        if not strip_tz(ref_df.iloc[i]).equals(strip_tz(r.drop('Sel'))):
                            try:
                                ini_dt = pd.to_datetime(r.get('inicio_incidente'))
                                if pd.isnull(r.get('fin_incidente')):
                                    fin_dt_sql = None
                                    dur_u = 0
                                    con_u = "Parcial"
                                else:
                                    fin_dt = pd.to_datetime(r.get('fin_incidente'))
                                    fin_dt_sql = fin_dt
                                    dur_u  = max(0, round((fin_dt - ini_dt).total_seconds() / 3600, 2))
                                    con_u = "Total"
                            except: 
                                fin_dt_sql = None
                                dur_u = 0
                                con_u = "Parcial"
                                
                            conn.execute(text("""
                                UPDATE incidents SET zona=:z, subzona=:sz, afectacion_general=:ag, servicio=:s, categoria=:c, equipo_afectado=:e, estado=:est, inicio_incidente=:idi, fin_incidente=:idf, clientes_afectados=:cl, causa_raiz=:cr, descripcion=:d, duracion_horas=:dur, conocimiento_tiempos=:con WHERE id=:id
                            """), {"z": r.get('zona',''), "sz": r.get('subzona',''), "ag": bool(r.get('afectacion_general', True)), "s": r.get('servicio',''), "c": r.get('categoria',''), "e": r.get('equipo_afectado',''), "est": r.get('estado','Cerrado'), "idi": ini_dt, "idf": fin_dt_sql, "cl": int(r.get('clientes_afectados', 0)), "cr": r.get('causa_raiz',''), "d": r.get('descripcion',''), "dur": dur_u, "con": con_u, "id": int(r['id'])})
                log_audit("UPDATE", "Edición masiva de registros.")
                load_data_rango.clear(); st.rerun()

            st.divider()
            st.download_button("📥 Descargar CSV de esta Tabla", df_d.drop(columns=drop_cols, errors='ignore').to_csv(index=False).encode(), f"NOC_Export_{fecha_ini}_{fecha_fin}.csv", "text/csv", use_container_width=True)

# ─────────────────────────────────────────────
# TAB 3 — CONFIGURACIÓN
# ─────────────────────────────────────────────
if role == 'admin' and len(tabs) > 3:
    with tabs[3]:
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
                        with engine.begin() as conn: conn.execute(text("DELETE FROM cat_zonas WHERE nombre=:n"), {"n": z_nombre})
                        clear_catalog_cache(); st.rerun()
                    except Exception as ex: st.error(str(ex))
            st.divider()
            with st.form("form_add_zona", clear_on_submit=True):
                st.markdown("**➕ Agregar Nueva Zona**")
                nz = st.text_input("Nombre del Nodo Principal")
                cla, clo = st.columns(2)
                nlat = cla.number_input("Latitud", value=13.6929, format="%.4f")
                nlon = clo.number_input("Longitud", value=-89.2182, format="%.4f")
                if st.form_submit_button("Agregar Zona") and nz:
                    try:
                        with engine.begin() as conn: conn.execute(text("INSERT INTO cat_zonas (nombre,lat,lon) VALUES (:n,:la,:lo)"), {"n": nz, "la": nlat, "lo": nlon})
                        clear_catalog_cache(); st.rerun()
                    except: st.error("Error: Zona duplicada.")

        with t_equipos:
            st.markdown("#### Gestión de Equipos de Red")
            for eq in get_cat("cat_equipos"):
                c_en, c_edel = st.columns([5,1])
                c_en.text(f"🖥️ {eq}")
                if c_edel.button("🗑️ Eliminar", key=f"del_eq_{eq}"):
                    try:
                        with engine.begin() as conn: conn.execute(text("DELETE FROM cat_equipos WHERE nombre=:n"), {"n": eq})
                        clear_catalog_cache(); st.rerun()
                    except Exception as ex: st.error(str(ex))
            st.divider()
            with st.form("form_add_eq", clear_on_submit=True):
                st.markdown("**➕ Agregar Nuevo Equipo**")
                ne = st.text_input("Nombre del dispositivo/equipo")
                if st.form_submit_button("Agregar Equipo") and ne:
                    try:
                        with engine.begin() as conn: conn.execute(text("INSERT INTO cat_equipos (nombre) VALUES (:n)"), {"n": ne})
                        clear_catalog_cache(); st.rerun()
                    except: st.error("Error: Equipo duplicado.")

        with t_causas:
            st.markdown("#### Gestión de Causas Raíz")
            for causa, ext in get_causas_con_flag().items():
                c_cn, c_ct, c_cdel = st.columns([4,2,1])
                c_cn.text(f"🛠️ {causa}")
                c_ct.caption("Externa (Fuerza Mayor)" if ext else "Interna (NOC/Infraestructura)")
                if c_cdel.button("🗑️ Eliminar", key=f"del_ca_{causa}"):
                    try:
                        with engine.begin() as conn: conn.execute(text("DELETE FROM cat_causas WHERE nombre=:n"), {"n": causa})
                        clear_catalog_cache(); st.rerun()
                    except Exception as e: st.error(str(e))
            st.divider()
            with st.form("form_add_causa", clear_on_submit=True):
                st.markdown("**➕ Agregar Nueva Causa**")
                nc  = st.text_input("Descripción de la causa")
                nce = st.checkbox("¿Es un factor Externo (Terceros, Clima, etc)?")
                if st.form_submit_button("Agregar Causa") and nc:
                    try:
                        with engine.begin() as conn: conn.execute(text("INSERT INTO cat_causas (nombre,es_externa) VALUES (:n,:e)"), {"n": nc, "e": nce})
                        clear_catalog_cache(); st.rerun()
                    except: st.error("Error: Causa duplicada.")

        with t_usuarios:
            st.markdown("#### Control de Accesos y Gestión de Contraseñas")
            cu, clg = st.columns([1, 2], gap="large")
            with cu:
                with st.form("form_u", clear_on_submit=True):
                    st.markdown("**Crear Usuario**")
                    nu    = st.text_input("Usuario")
                    np_u  = st.text_input("Contraseña", type="password")
                    nrl   = st.selectbox("Rol Asignado", ["viewer", "auditor", "admin"])
                    if st.form_submit_button("Crear Cuenta") and nu and np_u:
                        try:
                            with engine.begin() as conn:
                                conn.execute(text("INSERT INTO users (username,password_hash,role) VALUES (:u,:h,:r)"), {"u": nu, "h": hash_pw(np_u), "r": nrl})
                            st.toast("✅ Usuario creado exitosamente."); time.sleep(0.5); st.rerun()
                        except: st.toast("❌ Error: Nombre de usuario duplicado.", icon="❌")
                
                with st.form("form_reset_pw", clear_on_submit=True):
                    st.markdown("**Restablecer Contraseña (Admin)**")
                    u_reset = st.text_input("Escriba el nombre de usuario exacto")
                    p_reset = st.text_input("Nueva Contraseña", type="password")
                    if st.form_submit_button("Restablecer") and u_reset and p_reset:
                        try:
                            with engine.begin() as conn:
                                res = conn.execute(text("UPDATE users SET password_hash=:h, failed_attempts=0, locked_until=NULL WHERE username=:u"), {"h": hash_pw(p_reset), "u": u_reset})
                                if res.rowcount > 0: st.toast("✅ Contraseña actualizada.")
                                else: st.toast("❌ Usuario no encontrado.", icon="❌")
                        except Exception as e: st.toast(f"Error: {e}")

            with clg:
                try:
                    with engine.connect() as conn:
                        df_usrs = pd.read_sql(text("SELECT id,username,role,is_banned,failed_attempts FROM users"), conn)
                    df_usrs.insert(0, "Sel", False)
                    ed_usrs = st.data_editor(
                        df_usrs,
                        column_config={"Sel": st.column_config.CheckboxColumn("✔", default=False), "id": None, "username": "Usuario", "role": "Rol", "is_banned": "Baneado", "failed_attempts":"Intentos Fallidos"},
                        use_container_width=True, hide_index=True,
                    )
                    filas_del = ed_usrs[ed_usrs["Sel"] == True]
                    hay_cambios = not (df_usrs.drop(columns=['Sel']).reset_index(drop=True).equals(ed_usrs.drop(columns=['Sel']).reset_index(drop=True)))

                    u1c, u2c = st.columns(2)
                    if not filas_del.empty and u1c.button("🗑️ Eliminar Usuario", use_container_width=True):
                        if "Admin" in filas_del['username'].values:
                            st.error("No se puede eliminar la cuenta Admin raíz.")
                        else:
                            with engine.begin() as conn:
                                for rid in filas_del['id']: conn.execute(text("DELETE FROM users WHERE id=:id"), {"id": int(rid)})
                            st.rerun()

                    if hay_cambios and u2c.button("💾 Guardar Permisos", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for i, er in ed_usrs.iterrows():
                                orig = df_usrs.drop(columns=['Sel']).iloc[i]
                                if not orig.equals(er.drop('Sel')): conn.execute(text("UPDATE users SET role=:r,is_banned=:b,failed_attempts=:f WHERE id=:id"), {"r": str(er['role']), "b": bool(er['is_banned']), "f": int(er['failed_attempts']), "id": int(er['id'])})
                        st.rerun()
                except Exception as e: st.error(str(e))

            st.divider()
            st.markdown("##### 📜 Registro del Sistema (Audit Log)")
            try:
                with engine.connect() as conn:
                    logs = pd.read_sql(text("SELECT timestamp AS Fecha, username AS Usuario, action AS Accion, details AS Detalles FROM audit_logs ORDER BY id DESC"), conn)
                t_lp  = max(1, math.ceil(len(logs)/10))
                p_log = st.number_input("Página de log", 1, t_lp, 1)
                st.dataframe(logs.iloc[(p_log-1)*10: p_log*10], use_container_width=True, hide_index=True)
            except Exception as e: st.warning(str(e))
