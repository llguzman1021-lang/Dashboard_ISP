import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time, bcrypt, math, pytz, calendar, json
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, date, time as datetime_time
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors as rl_colors
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
from reportlab.lib.units import cm

# =====================================================================
# CONFIGURACIÓN GLOBAL
# =====================================================================
st.set_page_config(page_title="Multinet NOC", layout="wide", page_icon="🌐")
SV_TZ = pytz.timezone('America/El_Salvador')

PALETA_CORP = ['#f15c22', '#1d2c59', '#29b09d', '#ff9f43', '#83c9ff', '#ff2b2b']

# Catálogos por defecto (solo para seed inicial de la BD)
DEFAULT_ZONAS = [
    ("El Rosario",           13.4886, -89.0256),
    ("ARG",                  13.4880, -89.3200),
    ("Tepezontes",           13.6214, -89.0125),
    ("La Libertad",          13.4883, -89.3200),
    ("El Tunco",             13.4930, -89.3830),
    ("Costa del Sol",        13.3039, -88.9450),
    ("Zacatecoluca",         13.5048, -88.8710),
    ("Zaragoza",             13.5850, -89.2890),
    ("Santiago Nonualco",    13.5186, -88.9442),
    ("Rio Mar",              13.4900, -89.3500),
    ("San Salvador (Central)",13.6929,-89.2182),
]
DEFAULT_EQUIPOS = [
    "ONT","Repetidor Wi-Fi","Antena Ubiquiti","OLT","Caja NAP",
    "RB/Mikrotik","Switch","Servidor","Fibra Principal","Sistema UNIFI",
]
DEFAULT_CAUSAS = [
    ("Corte de Fibra por Terceros",    True),
    ("Corte de Fibra (No Especificado)",False),
    ("Caída de Árboles sobre Fibra",   True),
    ("Falla de Energía Comercial",     True),
    ("Corrosión en Equipos",           False),
    ("Daños por Fauna",                True),
    ("Falla de Hardware",              False),
    ("Falla de Configuración",         False),
    ("Falla de Redundancia",           False),
    ("Saturación de Tráfico",          False),
    ("Saturación en Servidor UNIFI",   False),
    ("Falla de Inicio en UNIFI",       False),
    ("Mantenimiento Programado",       False),
    ("Vandalismo o Hurto",             True),
    ("Condiciones Climáticas",         True),
]
DEFAULT_SERVICIOS = [
    "Internet","Cable TV (CATV)","IPTV (Mnet+)","Internet/Cable TV","Aplicativos internos",
]
DEFAULT_CATEGORIAS = [
    "Red Multinet","Cliente Corporativo","Falla Interna (No afecta clientes)",
]
CAT_INTERNA = "Falla Interna (No afecta clientes)"

MESES = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
         "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]

st.markdown("""
<style>
div.stButton > button {
    border: none !important; outline: none !important;
    box-shadow: none !important; border-radius: 8px;
    width: 100%; font-weight: 600; transition: all 0.3s ease !important;
}
div.stButton > button:focus { border: none !important; outline: none !important; box-shadow: none !important; }
div.stButton > button:hover { transform: translateY(-2px); }
[data-testid="stMetricValue"] { color: #ffffff !important; font-size: 36px !important; font-weight: 800 !important; }
[data-testid="stMetricLabel"] { color: #a5a8b5 !important; font-size: 15px !important; font-weight: 500 !important; }
[data-testid="stMetricDelta"] svg { width: 22px; height: 22px; }
[data-testid="stMetricDelta"] div { font-size: 14px !important; font-weight: 600 !important; }
button[data-baseweb="tab"] {
    background-color: #1e1e2f !important; border-radius: 12px 12px 0 0 !important;
    margin-right: 10px !important; padding: 14px 28px !important;
    border: 2px solid #333 !important; border-bottom: none !important;
    transition: all 0.3s;
}
button[data-baseweb="tab"]:hover { background-color: #2a2a3f !important; }
button[data-baseweb="tab"][aria-selected="true"] { background-color: #f15c22 !important; border-color: #f15c22 !important; }
button[data-baseweb="tab"] p { font-size: 16px !important; font-weight: 700 !important; color: #a5a8b5 !important; margin: 0 !important; }
button[data-baseweb="tab"][aria-selected="true"] p { color: #ffffff !important; }
.st-emotion-cache-1wivap2 { padding-top: 1.5rem; }
</style>
""", unsafe_allow_html=True)

if 'form_reset' not in st.session_state:
    st.session_state.form_reset = 0

# =====================================================================
# BASE DE DATOS
# =====================================================================
@st.cache_resource
def get_engine():
    return create_engine(st.secrets["neon_dsn"], pool_pre_ping=True, pool_recycle=300)

engine = get_engine()

def hash_pw(p):   return bcrypt.hashpw(p.encode(), bcrypt.gensalt()).decode()
def check_pw(p,h): return bcrypt.checkpw(p.encode(), h.encode())

def init_db():
    with engine.begin() as conn:
        # --- USUARIOS ---
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE,
                password_hash VARCHAR(255),
                role VARCHAR(20),
                pregunta VARCHAR(255),
                respuesta VARCHAR(255),
                failed_attempts INT DEFAULT 0,
                locked_until TIMESTAMP,
                is_banned BOOLEAN DEFAULT FALSE
            )
        """))

        # --- AUDITORÍA ---
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS audit_logs (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP,
                username VARCHAR(50),
                action VARCHAR(50),
                details TEXT
            )
        """))

        # --- CATÁLOGOS ---
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS cat_zonas (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(150) UNIQUE NOT NULL,
                lat FLOAT DEFAULT 13.6929,
                lon FLOAT DEFAULT -89.2182
            )
        """))
        conn.execute(text("CREATE TABLE IF NOT EXISTS cat_equipos (id SERIAL PRIMARY KEY, nombre VARCHAR(100) UNIQUE NOT NULL)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS cat_causas (id SERIAL PRIMARY KEY, nombre VARCHAR(150) UNIQUE NOT NULL, es_externa BOOLEAN DEFAULT FALSE)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS cat_servicios (id SERIAL PRIMARY KEY, nombre VARCHAR(100) UNIQUE NOT NULL)"))

        # Seed catálogos
        if conn.execute(text("SELECT count(*) FROM cat_zonas")).scalar() == 0:
            for nombre, lat, lon in DEFAULT_ZONAS:
                conn.execute(text("INSERT INTO cat_zonas (nombre, lat, lon) VALUES (:n, :lat, :lon) ON CONFLICT DO NOTHING"), {"n": nombre, "lat": lat, "lon": lon})

        if conn.execute(text("SELECT count(*) FROM cat_equipos")).scalar() == 0:
            for eq in DEFAULT_EQUIPOS: conn.execute(text("INSERT INTO cat_equipos (nombre) VALUES (:n) ON CONFLICT DO NOTHING"), {"n": eq})

        if conn.execute(text("SELECT count(*) FROM cat_causas")).scalar() == 0:
            for nombre, ext in DEFAULT_CAUSAS: conn.execute(text("INSERT INTO cat_causas (nombre, es_externa) VALUES (:n, :e) ON CONFLICT DO NOTHING"), {"n": nombre, "e": ext})

        if conn.execute(text("SELECT count(*) FROM cat_servicios")).scalar() == 0:
            for srv in DEFAULT_SERVICIOS: conn.execute(text("INSERT INTO cat_servicios (nombre) VALUES (:n) ON CONFLICT DO NOTHING"), {"n": srv})

        # --- INCIDENTES ---
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS incidents (
                id SERIAL PRIMARY KEY,
                zona VARCHAR(150) NOT NULL,
                subzona VARCHAR(150),
                afectacion_general BOOLEAN DEFAULT TRUE,
                servicio VARCHAR(100),
                categoria VARCHAR(100),
                equipo_afectado VARCHAR(100),
                inicio_incidente TIMESTAMPTZ NOT NULL,
                fin_incidente TIMESTAMPTZ NOT NULL,
                clientes_afectados INT DEFAULT 0,
                causa_raiz VARCHAR(150),
                descripcion TEXT,
                duracion_horas FLOAT,
                conocimiento_tiempos VARCHAR(50) DEFAULT 'Total',
                deleted_at TIMESTAMPTZ DEFAULT NULL
            )
        """))
        conn.execute(text("ALTER TABLE incidents ADD COLUMN IF NOT EXISTS conocimiento_tiempos VARCHAR(50) DEFAULT 'Total'"))
        conn.execute(text("ALTER TABLE incidents ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ DEFAULT NULL"))

        conn.execute(text("CREATE TABLE IF NOT EXISTS incidents_history (id SERIAL PRIMARY KEY, incident_id INT, changed_by VARCHAR(50), changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, old_data TEXT, new_data TEXT)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS inventario_nodos (id SERIAL PRIMARY KEY, zona VARCHAR(150), subzona VARCHAR(150), equipo VARCHAR(100), clientes INT DEFAULT 0, UNIQUE(zona, subzona, equipo))"))

        # Admin por defecto
        if conn.execute(text("SELECT count(*) FROM users WHERE username='Admin'")).scalar() == 0:
            conn.execute(text("INSERT INTO users (username,password_hash,role,pregunta,respuesta) VALUES ('Admin',:h,'admin','¿Color favorito?','azul')"), {"h": hash_pw("Areakde5")})

try:
    init_db()
except Exception as e:
    st.error(f"Error DB Inicialización: {e}")

def get_zonas():
    try:
        with engine.connect() as c:
            rows = c.execute(text("SELECT nombre, lat, lon FROM cat_zonas ORDER BY nombre")).fetchall()
            return [(r[0], r[1], r[2]) for r in rows]
    except: return [(n, lat, lon) for n, lat, lon in DEFAULT_ZONAS]

def get_cat(tabla):
    try:
        with engine.connect() as c: return [r[0] for r in c.execute(text(f"SELECT nombre FROM {tabla} ORDER BY nombre")).fetchall()]
    except: return []

def get_causas_con_flag():
    try:
        with engine.connect() as c: return {r[0]: r[1] for r in c.execute(text("SELECT nombre, es_externa FROM cat_causas ORDER BY nombre")).fetchall()}
    except: return {}

# =====================================================================
# CARGA DE DATOS POR RANGO DE FECHAS (USADO PARA MES ACTUAL Y ANTERIOR)
# =====================================================================
@st.cache_data(ttl=60)
def load_data_rango(fecha_ini: date, fecha_fin: date, include_deleted=False, zona_filtro="Todas", serv_filtro="Todos", seg_filtro="Todos"):
    s_date = datetime.combine(fecha_ini, datetime_time(0, 0, 0))
    e_date = datetime.combine(fecha_fin, datetime_time(23, 59, 59))

    q = """
        SELECT * FROM incidents
        WHERE (
            (inicio_incidente >= :s AND inicio_incidente <= :e)
            OR (fin_incidente   >= :s AND fin_incidente   <= :e)
            OR (inicio_incidente <= :s AND fin_incidente  >= :e)
        )
    """
    if not include_deleted: q += " AND deleted_at IS NULL"
    else: q += " AND deleted_at IS NOT NULL"

    if zona_filtro  != "Todas": q += f" AND zona = '{zona_filtro}'"
    if serv_filtro  != "Todos": q += f" AND servicio = '{serv_filtro}'"
    if seg_filtro   != "Todos": q += f" AND categoria = '{seg_filtro}'"

    q += " ORDER BY inicio_incidente ASC"
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(q), conn, params={"s": s_date, "e": e_date})
    except:
        return pd.DataFrame()

# =====================================================================
# ENRIQUECIMIENTO
# =====================================================================
def enriquecer(df):
    if df.empty: return df
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    for col in ['inicio_incidente', 'fin_incidente']:
        df[col] = pd.to_datetime(df[col], utc=True).dt.tz_convert(SV_TZ)

    df['duracion_horas']     = pd.to_numeric(df['duracion_horas'], errors='coerce').fillna(0.0)
    df['clientes_afectados'] = pd.to_numeric(df['clientes_afectados'], errors='coerce').fillna(0).astype(int)

    causas_ext_map = get_causas_con_flag()

    def eval_sev(r):
        if r['categoria'] == CAT_INTERNA:         return '🟢 P4 (Interna)'
        if r['duracion_horas'] >= 12 or r['clientes_afectados'] >= 1000: return '🔴 P1 (Crítica)'
        if r['duracion_horas'] >= 4  or r['clientes_afectados'] >= 300:  return '🟠 P2 (Alta)'
        return '🟡 P3 (Media)'

    df['Severidad']     = df.apply(eval_sev, axis=1)
    df['es_externa']    = df['causa_raiz'].map(lambda x: causas_ext_map.get(x, False))
    df['zona_completa'] = df.apply(lambda r: f"{r['zona']} (General)" if r.get('afectacion_general', True) else f"{r['zona']} - {r['subzona']}", axis=1)
    return df

# =====================================================================
# CÁLCULO DE KPIs
# =====================================================================
def _merge_intervals(intervals):
    if not intervals: return []
    srt = sorted(intervals, key=lambda x: x[0])
    merged = [list(srt[0])]
    for s, e in srt[1:]:
        if s <= merged[-1][1]: merged[-1][1] = max(merged[-1][1], e)
        else: merged.append([s, e])
    return merged

def calc_kpis(df, fecha_ini: date, fecha_fin: date):
    h_tot   = ((datetime.combine(fecha_fin, datetime_time(23,59,59)) - datetime.combine(fecha_ini, datetime_time(0,0,0))).total_seconds()) / 3600.0
    rng_s   = SV_TZ.localize(datetime.combine(fecha_ini, datetime_time(0,0,0)))
    rng_e   = SV_TZ.localize(datetime.combine(fecha_fin, datetime_time(23,59,59)))

    base = {
        "sla": 100.0, "mttr": 0.0, "acd": 0.0,
        "total_fallas": 0, "clientes": 0, "p1": 0,
        "mtbf": h_tot, "mh": 0.0,
        "pendiente_sin_cierre": 0, "pendiente_sin_clientes": 0, "internas": 0,
        "fallas_exactas": 0,
    }
    if df.empty: return base

    df_int  = df[df['categoria'] == CAT_INTERNA]
    df_ext  = df[df['categoria'] != CAT_INTERNA]

    base['internas']      = len(df_int)
    base['total_fallas']  = len(df_ext)

    # Incompletos
    df_sin_t = df_ext[df_ext['conocimiento_tiempos'] != 'Total']
    base['pendiente_sin_cierre'] = len(df_sin_t)

    # Completos en tiempo
    df_exact = df_ext[df_ext['conocimiento_tiempos'] == 'Total']

    # Sin clientes
    df_sin_cl = df_exact[df_exact['clientes_afectados'] == 0]
    base['pendiente_sin_clientes'] = len(df_sin_cl)

    # Exactos y con impacto a cliente
    df_t1 = df_exact[df_exact['clientes_afectados'] > 0]
    base['fallas_exactas'] = len(df_t1)
    base['p1'] = int((df_ext['Severidad'] == '🔴 P1 (Crítica)').sum())

    if not df_t1.empty:
        base['mttr']    = float(df_t1['duracion_horas'].mean())
        base['clientes'] = int(df_t1['clientes_afectados'].sum())
        total_hc         = (df_t1['duracion_horas'] * df_t1['clientes_afectados']).sum()
        base['acd']     = float(total_hc / base['clientes']) if base['clientes'] > 0 else 0.0

    # SLA
    df_sla = df_exact.copy()
    base['mh'] = float(df_sla['duracion_horas'].max()) if not df_sla.empty else 0.0

    if not df_sla.empty:
        s_cl = df_sla['inicio_incidente'].clip(lower=rng_s)
        e_cl = df_sla['fin_incidente'].clip(upper=rng_e)
        valid = (s_cl <= e_cl)
        intervals = [[s, e] for s, e in zip(s_cl[valid], e_cl[valid])]
        merged    = _merge_intervals(intervals)
        t_down    = sum((e - s).total_seconds() for s, e in merged) / 3600.0
    else:
        t_down = 0.0

    base['sla']  = max(0.0, min(100.0, (h_tot - t_down) / h_tot * 100)) if h_tot > 0 else 100.0
    up_hrs       = h_tot - t_down
    n_exact_all  = len(df_sla)
    base['mtbf'] = float(up_hrs / n_exact_all) if n_exact_all > 0 else float(h_tot)

    return base

# =====================================================================
# AUDITORÍA
# =====================================================================
def log_audit(action, detail):
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO audit_logs (timestamp, username, action, details)
                VALUES (:t, :u, :a, :d)
            """), {"t": datetime.now(SV_TZ).replace(tzinfo=None),
                   "u": st.session_state.get("username", "?"),
                   "a": action, "d": detail})
    except:
        pass

# =====================================================================
# GENERACIÓN DE PDF
# =====================================================================
def generar_pdf(label_periodo, kpis, df):
    RL_ORANGE = rl_colors.HexColor('#f15c22')
    RL_BLUE   = rl_colors.HexColor('#1d2c59')
    RL_TEAL   = rl_colors.HexColor('#29b09d')
    RL_LIGHT  = rl_colors.HexColor('#f5f5f5')
    RL_GRAY   = rl_colors.HexColor('#888888')
    RL_LGRAY  = rl_colors.HexColor('#dddddd')

    def mk_style(name, size, font='Helvetica', color=rl_colors.black, **kw):
        return ParagraphStyle(name, fontSize=size, fontName=font, textColor=color, **kw)

    s_title  = mk_style('t',  22, 'Helvetica-Bold', RL_BLUE,   spaceAfter=2)
    s_sub    = mk_style('s',  11, 'Helvetica',        RL_GRAY,   spaceAfter=14)
    s_period = mk_style('pe', 11, 'Helvetica-Bold',  RL_ORANGE, spaceAfter=2)
    s_body   = mk_style('b',   9, 'Helvetica',        rl_colors.black, spaceAfter=4)
    s_sec    = mk_style('h',  13, 'Helvetica-Bold',  RL_BLUE,   spaceBefore=14, spaceAfter=6)
    s_foot   = mk_style('f',   7, 'Helvetica',        RL_GRAY)

    def tbl_style(hdr_color):
        return TableStyle([
            ('BACKGROUND',  (0,0), (-1,0),  hdr_color),
            ('TEXTCOLOR',   (0,0), (-1,0),  rl_colors.white),
            ('FONTNAME',    (0,0), (-1,0),  'Helvetica-Bold'),
            ('FONTSIZE',    (0,0), (-1,0),  9),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[RL_LIGHT, rl_colors.white]),
            ('FONTNAME',    (0,1), (-1,-1), 'Helvetica'),
            ('FONTSIZE',    (0,1), (-1,-1), 9),
            ('VALIGN',      (0,0), (-1,-1), 'MIDDLE'),
            ('GRID',        (0,0), (-1,-1), 0.4, RL_LGRAY),
            ('ROWHEIGHT',   (0,0), (-1,-1), 20),
            ('LEFTPADDING', (0,0), (-1,-1), 7),
            ('RIGHTPADDING',(0,0), (-1,-1), 7),
            ('TEXTCOLOR',   (1,1), (1,-1),  RL_ORANGE),
            ('FONTNAME',    (1,1), (1,-1),  'Helvetica-Bold'),
            ('ALIGN',       (1,0), (1,-1),  'CENTER'),
        ])

    buffer  = BytesIO()
    doc     = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm,  bottomMargin=2*cm)
    story   = []
    now_str = datetime.now(SV_TZ).strftime('%d/%m/%Y %H:%M')

    story += [
        Paragraph("MULTINET", s_title),
        Paragraph("Reporte Ejecutivo · Network Operations Center", s_sub),
        HRFlowable(width="100%", thickness=2, color=RL_ORANGE, spaceAfter=10),
        Paragraph(f"<b>Periodo:</b> {label_periodo}", s_period),
        Paragraph(f"<b>Generado:</b> {now_str} (hora El Salvador)", s_body),
        Spacer(1, 0.5*cm),
    ]

    story.append(Paragraph("1. Métricas Operativas Principales", s_sec))
    kpi_rows = [
        ['Indicador', 'Valor', 'Descripción'],
        ['SLA Global (Disponibilidad)',   f"{kpis['sla']:.3f}%",   'Intervalos fusionados; sin doble descuento'],
        ['MTTR Real',                     f"{kpis['mttr']:.2f} horas",'Tiempo promedio de resolución (clientes > 0)'],
        ['ACD (Afectación por Cliente)',  f"{kpis['acd']:.2f} horas",'Percepción real del usuario promedio'],
        ['Clientes Impactados',           f"{kpis['clientes']:,}", 'Total acumulado de usuarios afectados'],
        ['MTBF (Estabilidad)',            f"{kpis['mtbf']:.1f} horas",'Tiempo entre fallas — datos exactos'],
        ['Fallas P1 Críticas',           f"{kpis['p1']}",          '>12h duración o >1000 clientes'],
    ]
    t = Table(kpi_rows, colWidths=[5.5*cm, 3*cm, 8.5*cm])
    t.setStyle(tbl_style(RL_BLUE))
    story += [t, Spacer(1, 0.4*cm)]

    story.append(Paragraph("2. Salud de Datos / Pendientes", s_sec))
    pend_rows = [
        ['Estado',                    'Cantidad'],
        ['Fallas sin tiempo de cierre',str(kpis['pendiente_sin_cierre'])],
        ['Fallas sin clientes definidos',str(kpis['pendiente_sin_clientes'])],
        ['Incidentes internos',        str(kpis['internas'])],
    ]
    tp = Table(pend_rows, colWidths=[10*cm, 4*cm])
    tp.setStyle(tbl_style(RL_TEAL))
    story += [tp, Spacer(1, 0.4*cm)]

    if not df.empty:
        df_ext = df[df['categoria'] != CAT_INTERNA]
        story.append(Paragraph("3. Zonas con Mayor Afectación", s_sec))
        top_z = df_ext.groupby('zona_completa')['duracion_horas'].sum().nlargest(8).reset_index()
        if not top_z.empty:
            z_rows = [['Zona / Nodo', 'Horas', 'Días Equiv.']]
            for _, r in top_z.iterrows():
                z_rows.append([str(r['zona_completa']), f"{r['duracion_horas']:.1f} h", f"{r['duracion_horas']/24:.2f}"])
            tz = Table(z_rows, colWidths=[9*cm, 4*cm, 4*cm])
            zs = tbl_style(RL_TEAL)
            tz.setStyle(zs)
            story += [tz, Spacer(1, 0.4*cm)]

    story += [
        Spacer(1, 1*cm),
        HRFlowable(width="100%", thickness=0.5, color=RL_LGRAY, spaceAfter=6),
        Paragraph(f"MULTINET NOC  ·  {now_str}  ·  Documento Confidencial", s_foot),
    ]
    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()

# =====================================================================
# LOGIN
# =====================================================================
for k in ['logged_in','role','username','log_u','log_p','log_err','log_msg']:
    if k not in st.session_state:
        st.session_state[k] = False if k == 'logged_in' else ""

def do_login():
    u = st.session_state.log_u
    p = st.session_state.log_p
    try:
        with engine.begin() as conn:
            ud = conn.execute(text("""
                SELECT id,password_hash,role,failed_attempts,locked_until,is_banned
                FROM users WHERE username=:u
            """), {"u": u}).fetchone()
            if ud:
                uid, ph, rol, fa, ldt, ban = ud
                fa     = fa or 0
                now_sv = datetime.now(SV_TZ).replace(tzinfo=None)
                if ban:
                    st.session_state.log_err = "❌ Cuenta baneada permanentemente."
                elif ldt and ldt > now_sv:
                    mins = (ldt - now_sv).seconds // 60 + 1
                    st.session_state.log_err = f"⏳ Bloqueado. Intente en {mins} min."
                elif check_pw(p, ph):
                    conn.execute(text("UPDATE users SET failed_attempts=0,locked_until=NULL WHERE id=:id"), {"id": uid})
                    st.session_state.update({"logged_in": True, "role": rol, "username": u, "log_err": ""})
                    return
                else:
                    fa += 1
                    if fa >= 6:
                        conn.execute(text("UPDATE users SET is_banned=TRUE,failed_attempts=:f WHERE id=:id"), {"f": fa,"id": uid})
                        st.session_state.log_err = "❌ Cuenta bloqueada permanentemente."
                    elif fa % 3 == 0:
                        conn.execute(text("UPDATE users SET locked_until=:dt,failed_attempts=:f WHERE id=:id"),
                                     {"dt": now_sv + timedelta(minutes=5), "f": fa, "id": uid})
                        st.session_state.log_err = "⏳ Bloqueado 5 min."
                    else:
                        conn.execute(text("UPDATE users SET failed_attempts=:f WHERE id=:id"), {"f": fa, "id": uid})
                        st.session_state.log_err = f"❌ Incorrecto. Intento {fa}/6."
            else:
                st.session_state.log_err = "❌ Usuario o contraseña incorrectos."
    except Exception as e:
        st.session_state.log_err = f"Error: {e}"
    st.session_state.log_u = ""
    st.session_state.log_p = ""

if not st.session_state.logged_in:
    st.markdown("<div style='margin-top:10vh;'></div>", unsafe_allow_html=True)
    _, col_c, _ = st.columns([1, 1.2, 1])
    with col_c:
        if st.session_state.log_msg: st.toast(st.session_state.log_msg, icon="✅"); st.session_state.log_msg = ""
        if st.session_state.log_err: st.error(st.session_state.log_err);             st.session_state.log_err  = ""
        with st.container(border=True):
            st.markdown("""
                <div style='text-align:center;padding:20px 0 10px 0;'>
                <div style='font-size:46px;'>🔐</div>
                <h2 style='margin:10px 0 4px;color:#fff;font-weight:700;'>Acceso NOC Central</h2>
                </div>
            """, unsafe_allow_html=True)
            st.text_input("Usuario",     key="log_u")
            st.text_input("Contraseña",  key="log_p", type="password")
            st.write("")
            _, btn_col, _ = st.columns([1, 2, 1])
            with btn_col:
                st.button("Iniciar Sesión", type="primary", on_click=do_login, use_container_width=True)
            st.write("")
            with st.expander("¿Olvidó su contraseña?"):
                ru = st.text_input("Ingrese su usuario:")
                if ru:
                    try:
                        with engine.connect() as conn:
                            ud2 = conn.execute(text("SELECT pregunta,respuesta FROM users WHERE username=:u"), {"u": ru}).fetchone()
                        if ud2:
                            st.info(f"**Pregunta:** {ud2[0]}")
                            rr = st.text_input("Respuesta:", type="password")
                            if rr:
                                if rr.strip().lower() == str(ud2[1]).lower():
                                    np_r = st.text_input("Nueva contraseña:", type="password")
                                    if st.button("Actualizar contraseña") and np_r:
                                        with engine.begin() as c2:
                                            c2.execute(text("""
                                                UPDATE users SET password_hash=:h,
                                                failed_attempts=0, locked_until=NULL, is_banned=FALSE
                                                WHERE username=:u
                                            """), {"h": hash_pw(np_r), "u": ru})
                                        st.session_state.log_msg = "Restablecida correctamente."
                                        st.rerun()
                                else:
                                    st.error("❌ Respuesta incorrecta.")
                        else:
                            st.error("❌ Usuario no encontrado.")
                    except:
                        pass
    st.stop()

# =====================================================================
# SIDEBAR
# =====================================================================
with st.sidebar:
    st.caption(f"👤 **{st.session_state.username}** ({st.session_state.role.capitalize()})  |  NOC v23.0")

    anio_act = datetime.now(SV_TZ).year
    anios    = sorted(list(set([anio_act+1, anio_act, anio_act-1, anio_act-2])), reverse=True)
    
    a_sel = st.selectbox("🗓️ Año", anios, index=anios.index(anio_act))
    m_sel = st.selectbox("📅 Mes", MESES, index=datetime.now(SV_TZ).month - 1)
    m_idx = MESES.index(m_sel) + 1
    
    last_day = calendar.monthrange(a_sel, m_idx)[1]
    fecha_ini = date(a_sel, m_idx, 1)
    fecha_fin = date(a_sel, m_idx, last_day)

    label_periodo = f"{m_sel} {a_sel}"

    st.divider()
    zonas_lista = [z[0] for z in get_zonas()]
    z_sel   = st.selectbox("🗺️ Zona",     ["Todas"]  + zonas_lista)
    srv_sel = st.selectbox("🌐 Servicio", ["Todos"]  + get_cat("cat_servicios"))
    seg_sel = st.selectbox("🏢 Segmento", ["Todos"]  + DEFAULT_CATEGORIAS)

    # Cargar datos del mes seleccionado
    df_m = enriquecer(load_data_rango(fecha_ini, fecha_fin,
                                      include_deleted=False,
                                      zona_filtro=z_sel,
                                      serv_filtro=srv_sel,
                                      seg_filtro=seg_sel))

    # Cargar datos del mes anterior (Para deltas comparativos)
    if m_idx == 1:
        p_m_idx = 12
        p_a_sel = a_sel - 1
    else:
        p_m_idx = m_idx - 1
        p_a_sel = a_sel

    p_last_day = calendar.monthrange(p_a_sel, p_m_idx)[1]
    p_fecha_ini = date(p_a_sel, p_m_idx, 1)
    p_fecha_fin = date(p_a_sel, p_m_idx, p_last_day)

    df_prev = enriquecer(load_data_rango(p_fecha_ini, p_fecha_fin,
                                         include_deleted=False,
                                         zona_filtro=z_sel,
                                         serv_filtro=srv_sel,
                                         seg_filtro=seg_sel))

    st.divider()
    if not df_m.empty:
        kpis_s   = calc_kpis(df_m, fecha_ini, fecha_fin)
        pdf_data = generar_pdf(label_periodo, kpis_s, df_m)
        st.download_button(
            label="📥 Descargar Reporte PDF",
            data=pdf_data,
            file_name=f"Reporte_NOC_{m_sel}_{a_sel}.pdf",
            mime="application/pdf",
            use_container_width=True,
        )
    else:
        st.info("Sin datos para reporte.")

    st.divider()
    if st.button("🚪 Cerrar Sesión", use_container_width=True):
        log_audit("LOGOUT", "Sesión cerrada.")
        st.session_state.clear()
        st.rerun()

# =====================================================================
# PESTAÑAS SEGÚN ROL
# =====================================================================
role = st.session_state.role

if role == 'admin':
    tab_labels = ["📊 Dashboard", "📝 Registrar Evento", "🗂️ Historial y Edición", "⚙️ Configuración"]
elif role == 'auditor':
    tab_labels = ["📊 Dashboard", "📝 Registrar Evento", "🗂️ Historial y Edición"]
else:  # viewer
    tab_labels = ["📊 Dashboard"]

tabs = st.tabs(tab_labels)

# ─────────────────────────────────────────────
# TAB 0 — DASHBOARD
# ─────────────────────────────────────────────
with tabs[0]:
    st.title(f"📊 Rendimiento de Red: {label_periodo}")

    if df_m.empty:
        st.success("🟢 Sin fallas registradas en los filtros seleccionados.")
    else:
        kpis = calc_kpis(df_m, fecha_ini, fecha_fin)
        kpis_prev = calc_kpis(df_prev, p_fecha_ini, p_fecha_fin) if not df_prev.empty else None

        # ── Función Helper para Deltas ──
        def get_delta(key, format_str="{:+.2f}", suffix=""):
            if not kpis_prev: return None
            diff = kpis[key] - kpis_prev[key]
            return f"{format_str.format(diff)} {suffix}"

        # ── 1. Estado General de la Red ──
        st.markdown("### 📈 Estado General de la Red")
        st.caption("*Métricas globales calculadas con registros que poseen duración exacta.*")

        c1, c2, c3, c4 = st.columns(4)
        
        c1.metric("SLA Global (Disponibilidad)", 
                  f"{kpis['sla']:.3f}%",
                  delta=get_delta('sla', "{:+.3f}", "%"), delta_color="normal",
                  help="Porcentaje del tiempo que la red estuvo operativa. Cruza intervalos de tiempo para evitar restar doble cuando múltiples nodos caen a la vez.")
        
        c2.metric("Total Eventos Registrados", 
                  kpis['total_fallas'],
                  delta=get_delta('total_fallas', "{:+.0f}"), delta_color="inverse",
                  help="Suma de todas las fallas externas registradas en el mes sin importar su calidad de dato.")
        
        c3.metric("MTBF (Estabilidad)", 
                  f"{kpis['mtbf']:.1f} horas",
                  delta=get_delta('mtbf', "{:+.1f}", "horas"), delta_color="normal",
                  help="Tiempo Medio Entre Fallas. Promedio de horas que la red opera sin interrupciones.")
        
        c4.metric("Falla Mayor (Pico)", 
                  f"{kpis['mh']:.1f} horas",
                  delta=get_delta('mh', "{:+.1f}", "horas"), delta_color="inverse",
                  help="Duración de la interrupción más larga registrada en el mes.")

        st.divider()

        # ── 2. Impacto a Clientes ──
        st.markdown("### 👥 Impacto Directo a Clientes (Datos Exactos)")
        st.caption("*Solo se contemplan eventos donde el técnico registró duración exacta y clientes afectados > 0.*")

        if kpis['fallas_exactas'] == 0:
            st.info("ℹ️ No hay incidentes masivos con tiempos exactos en este periodo.")
        else:
            t1a, t1b, t1c, t1d = st.columns(4)
            t1a.metric("Fallas Completas", 
                       kpis['fallas_exactas'],
                       delta=get_delta('fallas_exactas', "{:+.0f}"), delta_color="inverse",
                       help="Eventos que poseen tanto duración exacta como un número real de clientes > 0.")
            
            t1b.metric("MTTR Real (Resolución)", 
                       f"{kpis['mttr']:.2f} horas",
                       delta=get_delta('mttr', "{:+.2f}", "horas"), delta_color="inverse",
                       help="Tiempo Promedio de Resolución. Lo que tardan los técnicos en arreglar problemas que afectan a clientes finales.")
            
            t1c.metric("ACD Real (Afectación)", 
                       f"{kpis['acd']:.2f} horas",
                       delta=get_delta('acd', "{:+.2f}", "horas"), delta_color="inverse",
                       help="Afectación Promedio por Cliente. Es la sensación de corte real experimentada ponderada por volumen de usuarios.")
            
            t1d.metric("Clientes Afectados", 
                       f"{kpis['clientes']:,}",
                       delta=get_delta('clientes', "{:+.0f}"), delta_color="inverse",
                       help="Suma total de clientes que sufrieron cortes en este mes.")

        st.divider()

        # ── 3. Infraestructura y Datos Faltantes ──
        st.markdown("### 📡 Detalles de Infraestructura y Datos Faltantes")
        st.caption("*Eventos aislados, controlados o que requieren actualización en la bitácora.*")

        col_t2, col_t3, col_int = st.columns(3)
        
        with col_t2:
            with st.container(border=True):
                v_t2 = kpis['pendiente_sin_clientes']
                color = "🟡" if v_t2 > 0 else "🟢"
                st.markdown(f"#### {color} Fallas de Red (Sin Clientes)")
                st.metric("Total Eventos", v_t2, help="Caídas reportadas con tiempo exacto, pero marcadas con 0 clientes afectados. Afectan al SLA pero no al ACD.")
                
        with col_t3:
            with st.container(border=True):
                v_t3 = kpis['pendiente_sin_cierre']
                color = "🔴" if v_t3 > 0 else "🟢"
                st.markdown(f"#### {color} Eventos Incompletos")
                st.metric("Fallas Sin Tiempo", v_t3, help="Falta hora de inicio o cierre. Se asume duración 0. Completamente excluidas del SLA.")
                
        with col_int:
            with st.container(border=True):
                v_int = kpis['internas']
                color = "🔵" if v_int > 0 else "🟢"
                st.markdown(f"#### {color} Internas / Mantenimiento")
                st.metric("Eventos Internos", v_int, help="Problemas controlados o de oficinas que no impactan el servicio externo del ISP.")

        st.divider()

        # ── 4. Gráficos (Verticales) ──
        st.markdown("### 🗺️ Análisis Geográfico y Temporal")
        
        zonas_coords = {z[0]: (z[1], z[2]) for z in get_zonas()}
        
        df_map = df_m.copy()
        df_map['lat'] = df_map['zona'].map(lambda x: zonas_coords.get(x, (13.6929, -89.2182))[0])
        df_map['lon'] = df_map['zona'].map(lambda x: zonas_coords.get(x, (13.6929, -89.2182))[1])
        agg = (df_map.groupby(['zona_completa','lat','lon']).agg(Horas=('duracion_horas','sum'), Clientes=('clientes_afectados','sum')).reset_index())
        agg['Clientes_sz'] = agg['Clientes'].clip(lower=1)
        
        fig_m = px.scatter_mapbox(
            agg, lat="lat", lon="lon", hover_name="zona_completa", size="Clientes_sz", color="Horas",
            color_continuous_scale="Inferno", zoom=8.5, mapbox_style="carto-darkmatter", labels={"Clientes_sz": "Clientes"}, title="Impacto Geográfico (Zonas Afectadas)"
        )
        fig_m.update_layout(margin=dict(l=0,r=0,t=40,b=0), height=450)
        st.plotly_chart(fig_m, use_container_width=True)

        st.write("")

        dias_map   = {0:'Lunes',1:'Martes',2:'Miércoles',3:'Jueves',4:'Viernes',5:'Sábado',6:'Domingo'}
        dias_orden = list(dias_map.values())
        df_t2      = df_m.copy()
        df_t2['Día']  = pd.Categorical(df_t2['inicio_incidente'].dt.dayofweek.map(dias_map), categories=dias_orden, ordered=True)
        df_t2['Hora'] = df_t2['inicio_incidente'].dt.hour
        df_hm         = df_t2.groupby(['Día','Hora']).size().reset_index(name='Fallas')
        
        fig_hm = px.density_heatmap(df_hm, x='Hora', y='Día', z='Fallas', color_continuous_scale='Blues', nbinsx=24, title="Mapa de Calor (Concentración de Fallas por Hora)")
        fig_hm.update_layout(xaxis=dict(tickmode='linear', tick0=0, dtick=1), margin=dict(l=0,r=0,t=40,b=0), paper_bgcolor="rgba(0,0,0,0)", height=350)
        st.plotly_chart(fig_hm, use_container_width=True)

        st.write("")
        st.divider()
        st.markdown("### 📊 Responsabilidad y Causas Principales")
        
        df_m['Tipo'] = df_m['es_externa'].map({True: 'Externa (Fuerza Mayor)', False: 'Interna (Infraestructura / NOC)'})
        agg_r = df_m.groupby('Tipo').size().reset_index(name='Eventos')
        fig_p = px.pie(agg_r, names='Tipo', values='Eventos', hole=0.5, color_discrete_sequence=['#29b09d','#ff2b2b'], title="Tasa de Responsabilidad")
        fig_p.update_traces(textinfo='percent', textposition='inside')
        fig_p.update_layout(showlegend=True, legend=dict(orientation="h", yanchor="top", y=-0.1, xanchor="center", x=0.5), margin=dict(l=0,r=0,t=40,b=0), paper_bgcolor="rgba(0,0,0,0)", height=400)
        st.plotly_chart(fig_p, use_container_width=True)

        st.write("")
        
        dc = (df_m.groupby('causa_raiz').size().reset_index(name='Alertas').sort_values('Alertas', ascending=True).tail(8))
        fig_b = px.bar(dc, x='Alertas', y='causa_raiz', orientation='h', color_discrete_sequence=['#f15c22'], text_auto='.0f', title="Top Causas Raíz")
        fig_b.update_traces(textposition='outside')
        fig_b.update_layout(margin=dict(l=0,r=0,t=40,b=0), paper_bgcolor="rgba(0,0,0,0)", height=400, xaxis_title="", yaxis_title="")
        st.plotly_chart(fig_b, use_container_width=True)

# ─────────────────────────────────────────────
# TAB 1 — REGISTRAR EVENTO (admin + auditor)
# ─────────────────────────────────────────────
if role in ('admin', 'auditor') and len(tabs) > 1:
    with tabs[1]:
        st.title("📝 Registrar Evento Operativo")
        fk = st.session_state.form_reset

        try:
            cmdb_df = pd.read_sql("SELECT zona, subzona, equipo, clientes FROM inventario_nodos", engine)
        except:
            cmdb_df = pd.DataFrame(columns=['zona','subzona','equipo','clientes'])

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

                sz = st.text_input("📍 Sub-zona (Ej. San Antonio Masahuat)",
                                   value="General" if ag else "",
                                   key=f"sz_{fk}", disabled=ag)
                st.divider()

                c1f, c2f = st.columns(2)
                srv_f = c1f.selectbox("🌐 Servicio",  servs_form,      key=f"s_{fk}")
                cat_f = c2f.selectbox("🏢 Segmento",  DEFAULT_CATEGORIAS, key=f"c_{fk}")
                eq_f  = st.selectbox("🖥️ Equipo",     equipos_form,    key=f"e_{fk}")
                st.divider()

                ct1, ct2 = st.columns(2)
                with ct1:
                    fi  = st.date_input("📅 Fecha de Inicio", key=f"fi_{fk}")
                    hi_on = st.toggle("🕒 Asignar Hora de Inicio", value=False, key=f"hi_on_{fk}")
                    if hi_on:
                        hi_val = st.time_input("Hora Apertura", key=f"hi_val_{fk}")
                    else:
                        hi_val = None
                        st.info("ℹ️ Sin hora de inicio exacta → se marca como Incompleto.")
                with ct2:
                    ff  = st.date_input("📅 Fecha de Cierre", key=f"ff_{fk}")
                    hf_on = st.toggle("🕒 Asignar Hora de Cierre", value=False, key=f"hf_on_{fk}")
                    if hf_on:
                        hf_val = st.time_input("Hora Cierre", key=f"hf_val_{fk}")
                    else:
                        hf_val = None
                        st.info("ℹ️ Sin hora de cierre → se marca como Incompleto.")

                dur = 0.0
                conocimiento = "Parcial"
                if hi_on and hf_on:
                    dt_ini = datetime.combine(fi, hi_val)
                    dt_fin = datetime.combine(ff, hf_val)
                    if dt_fin > dt_ini:
                        dur = round((dt_fin - dt_ini).total_seconds() / 3600, 2)
                        conocimiento = "Total"

                st.divider()
                # ── Lógica estricta de clientes según segmento ──
                if cat_f == "Cliente Corporativo":
                    cl_f = st.number_input("👤 Clientes Afectados", value=1, disabled=True, key=f"cl_{fk}")
                    st.caption("🏢 Segmento Corporativo: Fijado en 1 enlace automáticamente.")
                elif cat_f == CAT_INTERNA:
                    cl_f = st.number_input("👤 Clientes Afectados", value=0, disabled=True, key=f"cl_{fk}")
                    st.caption("🔧 Falla Interna: Fijado en 0 clientes automáticamente.")
                else:
                    d_cl = 0
                    if not cmdb_df.empty:
                        sz_val = "General" if ag else sz
                        m = cmdb_df[(cmdb_df['zona'] == z_f) &
                                    (cmdb_df['subzona'] == sz_val) &
                                    (cmdb_df['equipo'] == eq_f)]
                        if not m.empty:
                            d_cl = int(m['clientes'].iloc[0])
                    cl_f = st.number_input("👤 Clientes Afectados (Deje 0 si no lo sabe)",
                                           min_value=0, value=d_cl, step=1, key=f"cl_{fk}")

                st.write("")
                cr_f   = st.selectbox("🛠️ Causa Raíz", causas_form, key=f"cr_{fk}")
                desc_f = st.text_area("📝 Descripción del Evento", key=f"desc_{fk}")

                if st.button("💾 Guardar Registro", type="primary"):
                    hi_db = hi_val if hi_on else datetime_time(0, 0)
                    hf_db = hf_val if hf_on else datetime_time(0, 0)
                    err   = False

                    if hi_on and hf_on:
                        if fi > ff or (fi == ff and hi_val >= hf_val):
                            st.toast("❌ Error: el cierre no puede ser anterior al inicio.", icon="🚨")
                            err = True

                    if not err:
                        with st.spinner("Guardando…"):
                            try:
                                idi = SV_TZ.localize(datetime.combine(fi, hi_db))
                                idf = SV_TZ.localize(datetime.combine(ff, hf_db))
                                sz_db = "General" if ag else sz

                                with engine.begin() as conn:
                                    conn.execute(text("""
                                        INSERT INTO incidents
                                        (zona, subzona, afectacion_general, servicio, categoria,
                                         equipo_afectado, inicio_incidente, fin_incidente,
                                         clientes_afectados, causa_raiz, descripcion,
                                         duracion_horas, conocimiento_tiempos)
                                        VALUES
                                        (:z, :sz, :ag, :s, :c, :e, :idi, :idf,
                                         :cl, :cr, :d, :dur, :con)
                                    """), {"z": z_f, "sz": sz_db, "ag": ag, "s": srv_f,
                                           "c": cat_f, "e": eq_f, "idi": idi, "idf": idf,
                                           "cl": cl_f, "cr": cr_f, "d": desc_f,
                                           "dur": dur, "con": conocimiento})

                                    conn.execute(text("""
                                        INSERT INTO inventario_nodos (zona, subzona, equipo, clientes)
                                        VALUES (:z, :sz, :e, :cl)
                                        ON CONFLICT (zona, subzona, equipo)
                                        DO UPDATE SET clientes = EXCLUDED.clientes
                                        WHERE EXCLUDED.clientes > inventario_nodos.clientes
                                    """), {"z": z_f, "sz": sz_db, "e": eq_f, "cl": cl_f})

                                log_audit("INSERT", f"Falla en {z_f}")
                                load_data_rango.clear()
                                st.session_state.form_reset += 1
                                st.rerun()
                            except Exception as e:
                                st.toast(f"Error: {e}", icon="❌")

        with ccx:
            st.markdown("#### 🕒 Registros Recientes")
            if not df_m.empty:
                for _, r in df_m.sort_values('id', ascending=False).head(6).iterrows():
                    with st.container(border=True):
                        ico = "🔧" if r.get('categoria') == CAT_INTERNA else "📡"
                        st.markdown(f"**{ico} {r['zona_completa']}**")
                        causa_str = str(r.get('causa_raiz',''))[:28]
                        st.caption(f"{causa_str}… | ⏳ {r['duracion_horas']} horas | 👥 {r['clientes_afectados']}")
            else:
                st.info("Sin registros en este mes.")

# ─────────────────────────────────────────────
# TAB 2 — HISTORIAL Y EDICIÓN (admin + auditor)
# ─────────────────────────────────────────────
if role in ('admin', 'auditor') and len(tabs) > 2:
    with tabs[2]:
        st.markdown("### 🗂️ Historial, Auditoría y Edición")
        papelera = st.toggle("🗑️ Explorar Papelera de Reciclaje")
        df_audit = enriquecer(load_data_rango(fecha_ini, fecha_fin,
                                              include_deleted=papelera,
                                              zona_filtro="Todas",
                                              serv_filtro="Todos",
                                              seg_filtro="Todos"))

        if df_audit.empty:
            st.info("No hay datos en el servidor para el periodo seleccionado.")
        else:
            c_s, c_pg = st.columns([4, 1])
            bq = c_s.text_input("🔎 Buscar:", placeholder="Causa, nodo, equipo…")
            df_d = (df_audit[df_audit.astype(str).apply(lambda x: x.str.contains(bq, case=False, na=False)).any(axis=1)].copy() if bq else df_audit.copy())

            tot_p = max(1, math.ceil(len(df_d)/15))
            pg    = c_pg.number_input("Página", 1, tot_p, 1, key="p_bd")
            df_page = df_d.iloc[(pg-1)*15 : pg*15].copy()
            df_page.insert(0, "Sel", False)

            drop_cols = [c for c in ['deleted_at','Severidad','zona_completa','data_quality_flag','es_externa'] if c in df_page.columns]

            ed_df = st.data_editor(
                df_page.drop(columns=drop_cols, errors='ignore'),
                column_config={
                    "Sel":              st.column_config.CheckboxColumn("✔", default=False),
                    "id":               None,
                    "inicio_incidente": st.column_config.DatetimeColumn("Inicio", format="YYYY-MM-DD HH:mm"),
                    "fin_incidente":    st.column_config.DatetimeColumn("Fin",    format="YYYY-MM-DD HH:mm"),
                },
                use_container_width=True, hide_index=True,
            )

            f_sel  = ed_df[ed_df["Sel"] == True]
            ref_df = (df_page.drop(columns=drop_cols + (['Sel'] if 'Sel' in drop_cols else []), errors='ignore').reset_index(drop=True))

            def strip_tz(s):
                if pd.api.types.is_datetime64_any_dtype(s):
                    return s.dt.tz_convert(None) if hasattr(s.dt, 'tz') and s.dt.tz else s
                return s

            ed_cmp  = ed_df.drop(columns=['Sel']).copy().apply(strip_tz)
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
                        load_data_rango.clear(); st.rerun()
                else:
                    if cb1.button("🗑️ Eliminar Seleccionados", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for rid in f_sel['id']:
                                conn.execute(text("UPDATE incidents SET deleted_at=CURRENT_TIMESTAMP WHERE id=:id"), {"id": int(rid)})
                        log_audit("DELETE (SOFT)", f"{len(f_sel)} registro(s).")
                        load_data_rango.clear(); st.rerun()

            if h_cam and cb2.button("💾 Guardar Cambios Editados", type="primary", use_container_width=True):
                with engine.begin() as conn:
                    for i, r in ed_df.iterrows():
                        o = ref_df.iloc[i]
                        r_no_sel = r.drop('Sel')
                        if not strip_tz(o).equals(strip_tz(r_no_sel)):
                            try:
                                ini_dt = pd.to_datetime(r.get('inicio_incidente'))
                                fin_dt = pd.to_datetime(r.get('fin_incidente'))
                                dur_u  = max(0, round((fin_dt - ini_dt).total_seconds() / 3600, 2)) if r.get('conocimiento_tiempos') == 'Total' else 0
                            except:
                                dur_u = 0
                            conn.execute(text("""
                                UPDATE incidents SET
                                    zona=:z, subzona=:sz, afectacion_general=:ag,
                                    servicio=:s, categoria=:c, equipo_afectado=:e,
                                    inicio_incidente=:idi, fin_incidente=:idf,
                                    clientes_afectados=:cl, causa_raiz=:cr,
                                    descripcion=:d, duracion_horas=:dur,
                                    conocimiento_tiempos=:con
                                WHERE id=:id
                            """), {
                                "z":   r.get('zona',''),
                                "sz":  r.get('subzona',''),
                                "ag":  bool(r.get('afectacion_general', True)),
                                "s":   r.get('servicio',''),
                                "c":   r.get('categoria',''),
                                "e":   r.get('equipo_afectado',''),
                                "idi": ini_dt,
                                "idf": fin_dt,
                                "cl":  int(r.get('clientes_afectados', 0)),
                                "cr":  r.get('causa_raiz',''),
                                "d":   r.get('descripcion',''),
                                "dur": dur_u,
                                "con": r.get('conocimiento_tiempos','Total'),
                                "id":  int(r['id']),
                            })
                log_audit("UPDATE", "Registros editados masivamente.")
                load_data_rango.clear(); st.rerun()

            st.divider()
            st.download_button(
                "📥 Descargar CSV de esta Tabla",
                df_d.drop(columns=drop_cols, errors='ignore').to_csv(index=False).encode(),
                f"NOC_Export_{fecha_ini}_{fecha_fin}.csv", "text/csv",
                use_container_width=True,
            )

# ─────────────────────────────────────────────
# TAB 3 — CONFIGURACIÓN (solo admin)
# ─────────────────────────────────────────────
if role == 'admin' and len(tabs) > 3:
    with tabs[3]:
        st.markdown("### ⚙️ Configuración del Sistema")
        st.caption("Administra los catálogos de opciones y gestiona los accesos de usuarios.")
        
        # Uso de sub-pestañas para evitar desorden visual
        t_zonas, t_equipos, t_causas, t_usuarios = st.tabs(["🗺️ Zonas", "🖥️ Equipos", "🛠️ Causas", "👤 Usuarios y Accesos"])

        # ── ZONAS ──
        with t_zonas:
            st.markdown("#### Gestión de Zonas (Nodos)")
            zonas_bd = get_zonas()
            for z_nombre, z_lat, z_lon in zonas_bd:
                c_zn, c_zdel = st.columns([5, 1])
                c_zn.text(f"📍 {z_nombre}  (Lat: {z_lat:.4f}, Lon: {z_lon:.4f})")
                if c_zdel.button("🗑️ Eliminar", key=f"del_z_{z_nombre}"):
                    try:
                        with engine.begin() as conn: conn.execute(text("DELETE FROM cat_zonas WHERE nombre=:n"), {"n": z_nombre})
                        st.rerun()
                    except Exception as e: st.error(str(e))
            st.divider()
            with st.form("form_add_zona", clear_on_submit=True):
                st.markdown("**➕ Agregar Nueva Zona**")
                nz    = st.text_input("Nombre del Nodo Principal")
                c_la, c_lo = st.columns(2)
                nlat  = c_la.number_input("Latitud",  value=13.6929, format="%.4f")
                nlon  = c_lo.number_input("Longitud", value=-89.2182, format="%.4f")
                if st.form_submit_button("Agregar Zona") and nz:
                    try:
                        with engine.begin() as conn: conn.execute(text("INSERT INTO cat_zonas (nombre,lat,lon) VALUES (:n,:la,:lo)"), {"n": nz, "la": nlat, "lo": nlon})
                        st.rerun()
                    except: st.error("Error: Zona duplicada.")

        # ── EQUIPOS ──
        with t_equipos:
            st.markdown("#### Gestión de Equipos de Red")
            for eq in get_cat("cat_equipos"):
                c_en, c_edel = st.columns([5,1])
                c_en.text(f"🖥️ {eq}")
                if c_edel.button("🗑️ Eliminar", key=f"del_eq_{eq}"):
                    try:
                        with engine.begin() as conn: conn.execute(text("DELETE FROM cat_equipos WHERE nombre=:n"), {"n": eq})
                        st.rerun()
                    except Exception as e: st.error(str(e))
            st.divider()
            with st.form("form_add_eq", clear_on_submit=True):
                st.markdown("**➕ Agregar Nuevo Equipo**")
                ne = st.text_input("Nombre del dispositivo/equipo")
                if st.form_submit_button("Agregar Equipo") and ne:
                    try:
                        with engine.begin() as conn: conn.execute(text("INSERT INTO cat_equipos (nombre) VALUES (:n)"), {"n": ne})
                        st.rerun()
                    except: st.error("Error: Equipo duplicado.")

        # ── CAUSAS ──
        with t_causas:
            st.markdown("#### Gestión de Causas Raíz")
            causas_bd = get_causas_con_flag()
            for causa, ext in causas_bd.items():
                c_cn, c_ct, c_cdel = st.columns([4,2,1])
                c_cn.text(f"🛠️ {causa}")
                c_ct.caption("Externa (Fuerza Mayor)" if ext else "Interna (NOC/Infraestructura)")
                if c_cdel.button("🗑️ Eliminar", key=f"del_ca_{causa}"):
                    try:
                        with engine.begin() as conn: conn.execute(text("DELETE FROM cat_causas WHERE nombre=:n"), {"n": causa})
                        st.rerun()
                    except Exception as e: st.error(str(e))
            st.divider()
            with st.form("form_add_causa", clear_on_submit=True):
                st.markdown("**➕ Agregar Nueva Causa**")
                nc  = st.text_input("Descripción de la causa")
                nce = st.checkbox("¿Es un factor Externo (Terceros, Clima, etc)?")
                if st.form_submit_button("Agregar Causa") and nc:
                    try:
                        with engine.begin() as conn: conn.execute(text("INSERT INTO cat_causas (nombre,es_externa) VALUES (:n,:e)"), {"n": nc, "e": nce})
                        st.rerun()
                    except: st.error("Error: Causa duplicada.")

        # ── USUARIOS Y LOGS ──
        with t_usuarios:
            st.markdown("#### Control de Accesos y Auditoría")
            cu, clg = st.columns([1, 2], gap="large")
            with cu:
                with st.form("form_u", clear_on_submit=True):
                    st.markdown("**Crear Usuario**")
                    nu    = st.text_input("Usuario")
                    np_u  = st.text_input("Contraseña", type="password")
                    nrl   = st.selectbox("Rol Asignado", ["viewer", "auditor", "admin"])
                    npr   = st.text_input("Pregunta de Seguridad (Opcional)")
                    nrs   = st.text_input("Respuesta")
                    if st.form_submit_button("Crear Cuenta") and nu and np_u:
                        try:
                            with engine.begin() as conn:
                                conn.execute(text("""
                                    INSERT INTO users (username,password_hash,role,pregunta,respuesta)
                                    VALUES (:u,:h,:r,:p,:rs)
                                """), {"u": nu, "h": hash_pw(np_u), "r": nrl, "p": npr, "rs": nrs})
                            st.toast("✅ Usuario creado exitosamente.")
                            time.sleep(0.5); st.rerun()
                        except: st.toast("❌ Error: Nombre de usuario duplicado.", icon="❌")

            with clg:
                try:
                    with engine.connect() as conn:
                        df_usrs = pd.read_sql(text("SELECT id,username,role,is_banned,failed_attempts FROM users"), conn)
                    df_usrs.insert(0, "Sel", False)
                    ed_usrs = st.data_editor(
                        df_usrs,
                        column_config={
                            "Sel": st.column_config.CheckboxColumn("✔", default=False),
                            "id": None, "username": "Usuario", "role": "Rol", "is_banned": "Baneado", "failed_attempts":"Intentos Fallidos"
                        },
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
                                if not orig.equals(er.drop('Sel')):
                                    conn.execute(text("""
                                        UPDATE users SET role=:r,is_banned=:b,failed_attempts=:f WHERE id=:id
                                    """), {"r": str(er['role']), "b": bool(er['is_banned']), "f": int(er['failed_attempts']), "id": int(er['id'])})
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
