import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time, bcrypt, math, pytz, calendar, json
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, time as datetime_time
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

ZONAS_SV = ["El Rosario", "ARG", "Tepezontes", "La Libertad", "El Tunco", "Costa del Sol", "Zacatecoluca", "Zaragoza", "Santiago Nonualco", "Rio Mar", "San Salvador (Central)"]
EQUIPOS_SV = ["ONT", "Repetidor Wi-Fi", "Antena Ubiquiti", "OLT", "Caja NAP", "RB/Mikrotik", "Switch", "Servidor", "Fibra Principal", "Sistema UNIFI"]
CAUSAS_RAIZ = ["Corte de Fibra por Terceros","Corte de Fibra (No Especificado)","Caída de Árboles sobre Fibra","Falla de Energía Comercial","Corrosión en Equipos","Daños por Fauna","Falla de Hardware","Falla de Configuración","Falla de Redundancia","Saturación de Tráfico","Saturación en Servidor UNIFI","Falla de Inicio en UNIFI","Mantenimiento Programado","Vandalismo o Hurto","Condiciones Climáticas"]
CAUSAS_EXTERNAS = ["Corte de Fibra por Terceros", "Corte de Fibra (No Especificado)", "Caída de Árboles sobre Fibra", "Falla de Energía Comercial", "Daños por Fauna", "Vandalismo o Hurto", "Condiciones Climáticas"]
CATEGORIAS = ["Red Multinet", "Cliente Corporativo", "Falla Interna (No afecta clientes)"]
SERVICIOS = ["Internet", "Cable TV (CATV)", "IPTV (Mnet+)", "Internet/Cable TV", "Aplicativos internos"]
CAT_INTERNA = "Falla Interna (No afecta clientes)"

COORDS = {
    "El Rosario": [13.4886, -89.0256], "ARG": [13.4880, -89.3200], "Tepezontes": [13.6214, -89.0125],
    "La Libertad": [13.4883, -89.3200], "El Tunco": [13.4930, -89.3830], "Costa del Sol": [13.3039, -88.9450],
    "Zacatecoluca": [13.5048, -88.8710], "Zaragoza": [13.5850, -89.2890], "Santiago Nonualco": [13.5186, -88.9442],
    "Rio Mar": [13.4900, -89.3500], "San Salvador (Central)": [13.6929, -89.2182]
}

st.markdown("""
    <style>
    div.stButton > button { border: none !important; outline: none !important; box-shadow: none !important; border-radius: 8px; width: 100%; font-weight: 600; transition: all 0.3s ease !important; }
    div.stButton > button:focus { border: none !important; outline: none !important; box-shadow: none !important; }
    div.stButton > button:hover { transform: translateY(-2px); }
    div[data-testid="stButton-delete"] > button { background-color: #c0392b !important; color: white !important; }
    div[data-testid="stButton-save"] > button  { background-color: #27ae60 !important; color: white !important; }
    div.stButton > button:first-child { background-color: #0068c9 !important; color: white !important; }
    [data-testid="stMetricValue"] { color: #ffffff !important; font-size: 36px !important; font-weight: 800 !important; }
    [data-testid="stMetricLabel"] { color: #a5a8b5 !important; font-size: 15px !important; font-weight: 500 !important; }
    div[data-testid="stTabs"] { background-color: transparent; }
    button[data-baseweb="tab"] { background-color: #1e1e2f !important; border-radius: 12px 12px 0 0 !important; margin-right: 10px !important; padding: 16px 32px !important; border: 2px solid #333 !important; border-bottom: none !important; transition: all 0.3s; }
    button[data-baseweb="tab"]:hover { background-color: #2a2a3f !important; }
    button[data-baseweb="tab"][aria-selected="true"] { background-color: #f15c22 !important; border-color: #f15c22 !important; }
    button[data-baseweb="tab"] p { font-size: 20px !important; font-weight: 700 !important; color: #a5a8b5 !important; margin: 0 !important; }
    button[data-baseweb="tab"][aria-selected="true"] p { color: #ffffff !important; }
    .st-emotion-cache-1wivap2 { padding-top: 1.5rem; }
    </style>
""", unsafe_allow_html=True)

if 'form_reset' not in st.session_state: st.session_state.form_reset = 0

# =====================================================================
# BASE DE DATOS E INICIALIZACIÓN
# =====================================================================
@st.cache_resource
def get_engine(): return create_engine(st.secrets["neon_dsn"], pool_pre_ping=True, pool_recycle=300)
engine = get_engine()

def hash_pw(p):  return bcrypt.hashpw(p.encode(), bcrypt.gensalt()).decode()
def check_pw(p, h): return bcrypt.checkpw(p.encode(), h.encode())

def init_db():
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, username VARCHAR(50) UNIQUE, password_hash VARCHAR(255), role VARCHAR(20), pregunta VARCHAR(255), respuesta VARCHAR(255), failed_attempts INT DEFAULT 0, locked_until TIMESTAMP, is_banned BOOLEAN DEFAULT FALSE);"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS audit_logs (id SERIAL PRIMARY KEY, timestamp TIMESTAMP, username VARCHAR(50), action VARCHAR(50), details TEXT);"))
        
        try:
            check = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='incidents' AND column_name='subzona'")).fetchall()
            if len(check) == 0:
                conn.execute(text("DROP TABLE IF EXISTS incidents CASCADE;"))
                conn.execute(text("DROP TABLE IF EXISTS incidents_history CASCADE;"))
                conn.execute(text("DROP TABLE IF EXISTS inventario_nodos CASCADE;"))
        except: pass

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS incidents (
                id SERIAL PRIMARY KEY,
                zona VARCHAR(100) NOT NULL,
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
        conn.execute(text("ALTER TABLE incidents ADD COLUMN IF NOT EXISTS conocimiento_tiempos VARCHAR(50) DEFAULT 'Total';"))
        
        conn.execute(text("CREATE TABLE IF NOT EXISTS incidents_history (id SERIAL PRIMARY KEY, incident_id INT, changed_by VARCHAR(50), changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, old_data TEXT, new_data TEXT)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS inventario_nodos (id SERIAL PRIMARY KEY, zona VARCHAR(100), subzona VARCHAR(150), equipo VARCHAR(100), clientes INT DEFAULT 0, UNIQUE(zona, subzona, equipo));"))

        if conn.execute(text("SELECT count(*) FROM users WHERE username='Admin'")).scalar() == 0:
            conn.execute(text("INSERT INTO users (username,password_hash,role,pregunta,respuesta) VALUES ('Admin',:h,'admin','¿Color favorito?','azul')"), {"h": hash_pw("Areakde5")})

try: init_db()
except Exception as e: st.error(f"Error DB Inicialización: {e}")

@st.cache_data(ttl=60)
def load_data_mes(m_idx, anio, include_deleted=False, zona_filtro="Todas", serv_filtro="Todos", seg_filtro="Todos"):
    s_date = f"{anio}-{m_idx:02d}-01 00:00:00"
    end_d = calendar.monthrange(anio, m_idx)[1]
    e_date = f"{anio}-{m_idx:02d}-{end_d} 23:59:59"
    
    q = "SELECT * FROM incidents WHERE ((inicio_incidente >= :s AND inicio_incidente <= :e) OR (fin_incidente >= :s AND fin_incidente <= :e) OR (inicio_incidente <= :s AND fin_incidente >= :e))"
    if not include_deleted: q += " AND deleted_at IS NULL"
    else: q += " AND deleted_at IS NOT NULL"
    if zona_filtro != "Todas": q += f" AND zona = '{zona_filtro}'"
    if serv_filtro != "Todos": q += f" AND servicio = '{serv_filtro}'"
    if seg_filtro != "Todos": q += f" AND categoria = '{seg_filtro}'"
    
    q += " ORDER BY inicio_incidente ASC"
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(q), conn, params={"s": s_date, "e": e_date})
    except: return pd.DataFrame()

# =====================================================================
# ENRIQUECIMIENTO Y NORMALIZACIÓN
# =====================================================================
def enriquecer_y_normalizar(df):
    if df.empty: return df
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    
    df['inicio_incidente'] = pd.to_datetime(df['inicio_incidente'])
    if df['inicio_incidente'].dt.tz is not None: df['inicio_incidente'] = df['inicio_incidente'].dt.tz_convert(SV_TZ)
    else: df['inicio_incidente'] = df['inicio_incidente'].dt.tz_localize(SV_TZ)
        
    df['fin_incidente'] = pd.to_datetime(df['fin_incidente'])
    if df['fin_incidente'].dt.tz is not None: df['fin_incidente'] = df['fin_incidente'].dt.tz_convert(SV_TZ)
    else: df['fin_incidente'] = df['fin_incidente'].dt.tz_localize(SV_TZ)

    df['duracion_horas']     = pd.to_numeric(df['duracion_horas'], errors='coerce').fillna(0.0)
    df['clientes_afectados'] = pd.to_numeric(df['clientes_afectados'], errors='coerce').fillna(0).astype(int)
    
    df.loc[df['clientes_afectados'] == 0, 'categoria'] = CAT_INTERNA
    df['data_quality_flag'] = (df['conocimiento_tiempos'] == 'Total')
    
    def eval_severidad(r):
        if r['categoria'] == CAT_INTERNA: return '🟢 P4 (Interna)'
        if r['duracion_horas'] >= 12 or r['clientes_afectados'] >= 1000: return '🔴 P1 (Crítica)'
        if r['duracion_horas'] >= 4 or r['clientes_afectados'] >= 300: return '🟠 P2 (Alta)'
        return '🟡 P3 (Media)'

    df['Severidad'] = df.apply(eval_severidad, axis=1)
    df['zona_completa'] = df.apply(lambda r: f"{r['zona']} (General)" if r['afectacion_general'] else f"{r['zona']} - {r['subzona']}", axis=1)
    return df

def calc_kpis(df, anio, m_idx):
    """Calcula las métricas estructuradas lógicamente por Niveles (Tiers)"""
    base = {
        "global": {"sla": 100.0, "total_fallas": 0, "mtbf": 0.0, "db": 0.0, "mh": 0.0, "p1": 0},
        "t1": {"fallas": 0, "mttr": 0.0, "acd": 0.0, "clientes": 0},
        "t2": {"fallas": 0, "mttr": 0.0},
        "t3": {"fallas": 0, "clientes_est": 0, "zona_top": "N/A"},
        "int": {"fallas": 0, "mttr": 0.0},
        "has_incomplete": False
    }
    
    if df.empty: return base
    
    h_tot = calendar.monthrange(anio, m_idx)[1] * 24
    m_start = SV_TZ.localize(datetime(anio, m_idx, 1, 0, 0, 0))
    m_end = SV_TZ.localize(datetime(anio, m_idx, calendar.monthrange(anio, m_idx)[1], 23, 59, 59))

    df_int = df[df['categoria'] == CAT_INTERNA]
    df_ext = df[df['categoria'] != CAT_INTERNA]

    # --- MÉTRICAS INTERNAS ---
    base["int"]["fallas"] = len(df_int)
    if not df_int.empty:
        iv = df_int[df_int['duracion_horas'] > 0]
        base["int"]["mttr"] = float(iv['duracion_horas'].mean()) if not iv.empty else 0.0

    if df_ext.empty: 
        base["global"]["mtbf"] = float(h_tot)
        return base

    # --- TIER 3 (Incompletas / Sin tiempos) ---
    df_t3 = df_ext[df_ext['conocimiento_tiempos'] != 'Total']
    base["has_incomplete"] = not df_t3.empty
    base["t3"]["fallas"] = len(df_t3)
    base["t3"]["clientes_est"] = int(df_t3['clientes_afectados'].sum())
    if not df_t3.empty: base["t3"]["zona_top"] = df_t3['zona_completa'].mode()[0]

    # --- TIER 2 (Medidas en Tiempo, Sin Clientes) ---
    df_t2 = df_ext[(df_ext['conocimiento_tiempos'] == 'Total') & (df_ext['clientes_afectados'] == 0)]
    base["t2"]["fallas"] = len(df_t2)
    if not df_t2.empty:
        base["t2"]["mttr"] = float(df_t2['duracion_horas'].mean())

    # --- TIER 1 (Perfectas: Tiempos y Clientes) ---
    df_t1 = df_ext[(df_ext['conocimiento_tiempos'] == 'Total') & (df_ext['clientes_afectados'] > 0)]
    base["t1"]["fallas"] = len(df_t1)
    if not df_t1.empty:
        base["t1"]["mttr"] = float(df_t1['duracion_horas'].mean())
        base["t1"]["clientes"] = int(df_t1['clientes_afectados'].sum())
        total_horas_cliente = (df_t1['duracion_horas'] * df_t1['clientes_afectados']).sum()
        base["t1"]["acd"] = float(total_horas_cliente / base["t1"]["clientes"]) if base["t1"]["clientes"] > 0 else 0.0

    # --- GLOBAL (SLA, MTBF, y Acumulados usan T1 + T2) ---
    base["global"]["total_fallas"] = len(df_ext)
    base["global"]["p1"] = len(df_ext[df_ext['Severidad'] == '🔴 P1 (Crítica)'])
    
    df_valid_times = pd.concat([df_t1, df_t2])
    t_real = 0.0
    if not df_valid_times.empty:
        base["global"]["db"] = float(df_valid_times['duracion_horas'].sum())
        base["global"]["mh"] = float(df_valid_times['duracion_horas'].max())
        
        # Calcular traslapes exactos para SLA
        s_dt = df_valid_times['inicio_incidente'].clip(lower=m_start)
        e_dt = df_valid_times['fin_incidente'].clip(upper=m_end)
        valid = s_dt.notna() & e_dt.notna() & (s_dt <= e_dt)
        if valid.any():
            intervals = sorted([[s, e] for s, e in zip(s_dt[valid], e_dt[valid])], key=lambda x: x[0])
            merged = [intervals[0]]
            for iv in intervals[1:]:
                if iv[0] <= merged[-1][1]: merged[-1][1] = max(merged[-1][1], iv[1])
                else: merged.append(iv)
            t_real = sum((en - st_).total_seconds() for st_, en in merged) / 3600.0

    base["global"]["sla"] = max(0.0, min(100.0, ((h_tot - t_real) / h_tot) * 100)) if h_tot > 0 else 100.0
    uptime_hrs = h_tot - t_real
    base["global"]["mtbf"] = float(uptime_hrs / len(df_valid_times)) if len(df_valid_times) > 0 else float(h_tot)

    return base

# =====================================================================
# AUDITORÍA Y PDF
# =====================================================================
def log_audit(action, detail):
    try:
        with engine.begin() as conn: conn.execute(text("INSERT INTO audit_logs (timestamp,username,action,details) VALUES (:t,:u,:a,:d)"), {"t": datetime.now(SV_TZ).replace(tzinfo=None), "u": st.session_state.username, "a": action, "d": detail})
    except: pass

def log_version(incident_id, old_d, new_d):
    try:
        with engine.begin() as conn: conn.execute(text("INSERT INTO incidents_history (incident_id, changed_by, old_data, new_data) VALUES (:i, :u, :o, :n)"), {"i": incident_id, "u": st.session_state.username, "o": json.dumps(old_d), "n": json.dumps(new_d)})
    except: pass

def generar_pdf(mes, anio, kpis, df):
    RL_ORANGE = rl_colors.HexColor('#f15c22'); RL_BLUE = rl_colors.HexColor('#1d2c59'); RL_TEAL = rl_colors.HexColor('#29b09d'); RL_LIGHT = rl_colors.HexColor('#f5f5f5'); RL_GRAY = rl_colors.HexColor('#888888'); RL_LGRAY = rl_colors.HexColor('#dddddd')
    def mk_style(name, size, font='Helvetica', color=rl_colors.black, **kw): return ParagraphStyle(name, fontSize=size, fontName=font, textColor=color, **kw)
    s_title = mk_style('t', 22, 'Helvetica-Bold', RL_BLUE, spaceAfter=2); s_sub = mk_style('s', 11, 'Helvetica', RL_GRAY, spaceAfter=14); s_period = mk_style('pe', 11, 'Helvetica-Bold', RL_ORANGE, spaceAfter=2); s_body = mk_style('b', 9, 'Helvetica', rl_colors.black, spaceAfter=4); s_sec = mk_style('h', 13, 'Helvetica-Bold', RL_BLUE, spaceBefore=14, spaceAfter=6); s_foot = mk_style('f', 7, 'Helvetica', RL_GRAY)
    def table_style(header_color, val_col=True):
        base = [('BACKGROUND', (0,0), (-1,0), header_color), ('TEXTCOLOR', (0,0), (-1,0), rl_colors.white), ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'), ('FONTSIZE', (0,0), (-1,0), 9), ('ROWBACKGROUNDS',(0,1), (-1,-1), [RL_LIGHT, rl_colors.white]), ('FONTNAME', (0,1), (-1,-1), 'Helvetica'), ('FONTSIZE', (0,1), (-1,-1), 9), ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('GRID', (0,0), (-1,-1), 0.4, RL_LGRAY), ('ROWHEIGHT', (0,0), (-1,-1), 20), ('LEFTPADDING', (0,0), (-1,-1), 7), ('RIGHTPADDING', (0,0), (-1,-1), 7)]
        if val_col: base += [('TEXTCOLOR', (1,1), (1,-1), RL_ORANGE), ('FONTNAME', (1,1), (1,-1), 'Helvetica-Bold'), ('ALIGN', (1,0), (1,-1), 'CENTER')]
        return TableStyle(base)
    buffer = BytesIO(); doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm); story = []; now_str = datetime.now(SV_TZ).strftime('%d/%m/%Y %H:%M')
    story.append(Paragraph("MULTINET", s_title)); story.append(Paragraph("Reporte Ejecutivo · Network Operations Center", s_sub)); story.append(HRFlowable(width="100%", thickness=2, color=RL_ORANGE, spaceAfter=10)); story.append(Paragraph(f"<b>Periodo:</b> {mes} {anio}", s_period)); story.append(Paragraph(f"<b>Generado:</b> {now_str} (hora El Salvador)", s_body)); story.append(Spacer(1, 0.5*cm))
    
    story.append(Paragraph("1. Indicadores Segmentados por Calidad de Dato", s_sec))
    kpi_rows = [
        ['Indicador', 'Valor', 'Descripción'], 
        ['SLA Global (Disponibilidad)', f"{kpis['global']['sla']:.2f}%", 'Operatividad calculada con datos exactos'], 
        ['Impacto Acumulado', f"{kpis['global']['db']/24:.2f} días", 'Total de horas caídas (T1+T2) / 24'],
        ['MTBF (Tiempo entre Fallas)', f"{kpis['global']['mtbf']:.1f} hrs", 'Tiempo promedio sin fallas medidas'],
        ['Fallas P1 (Críticas)', f"{kpis['global']['p1']}", 'Incidentes críticos globales'],
        ['T1: MTTR (Datos Exactos)', f"{kpis['t1']['mttr']:.2f} hrs", 'Resolución incidentes que afectaron clientes'], 
        ['T1: ACD Real', f"{kpis['t1']['acd']:.2f} hrs", 'Afectación ponderada por cliente'],
        ['T1: Clientes Afectados', f"{kpis['t1']['clientes']:,}", 'Volumen de usuarios impactados'],
        ['T2/T3: Fallas Sin Impacto/Hora', f"{kpis['t2']['fallas'] + kpis['t3']['fallas']}", 'Eventos excluidos de métricas de cliente']
    ]
    kpi_t = Table(kpi_rows, colWidths=[5.5*cm, 3*cm, 8.5*cm]); kpi_t.setStyle(table_style(RL_BLUE)); story.append(kpi_t); story.append(Spacer(1, 0.4*cm))
    
    if not df.empty:
        df_ext_p = df[df['categoria'] != CAT_INTERNA]; story.append(Paragraph("2. Zonas con Mayor Tiempo de Afectación", s_sec)); top_z = df_ext_p.groupby('zona_completa')['duracion_horas'].sum().nlargest(8).reset_index()
        if not top_z.empty:
            z_rows = [['Zona / Nodo', 'Horas de Caída', 'Días Equiv.']]
            for _, r in top_z.iterrows(): z_rows.append([str(r['zona_completa']), f"{r['duracion_horas']:.1f} hrs", f"{r['duracion_horas']/24:.2f}"])
            z_t = Table(z_rows, colWidths=[9*cm, 4.5*cm, 3.5*cm]); zs = table_style(RL_TEAL, val_col=False); zs.add('ALIGN', (1,0), (-1,-1), 'CENTER'); z_t.setStyle(zs); story.append(z_t)
        story.append(Spacer(1, 0.4*cm))
    story.append(Spacer(1, 1*cm)); story.append(HRFlowable(width="100%", thickness=0.5, color=RL_LGRAY, spaceAfter=6)); story.append(Paragraph(f"MULTINET NOC  ·  Generado el {now_str}  ·  Documento Confidencial", s_foot)); doc.build(story); buffer.seek(0)
    return buffer.getvalue()

# =====================================================================
# SISTEMA DE LOGIN Y SESIÓN
# =====================================================================
for k in ['logged_in','role','username','log_u','log_p','log_err','log_msg']:
    if k not in st.session_state: st.session_state[k] = False if k == 'logged_in' else ("" if "log" in k else None)

def do_login():
    u, p = st.session_state.log_u, st.session_state.log_p
    try:
        with engine.begin() as conn:
            ud = conn.execute(text("SELECT id,password_hash,role,failed_attempts,locked_until,is_banned FROM users WHERE username=:u"), {"u": u}).fetchone()
            if ud:
                uid, ph, rol, fa, ldt, ban = ud
                fa = fa or 0; now_sv = datetime.now(SV_TZ).replace(tzinfo=None)
                if ban: st.session_state.log_err = "❌ Cuenta baneada permanentemente."
                elif ldt and ldt > now_sv: st.session_state.log_err = f"⏳ Bloqueado. Intente en {(ldt - now_sv).seconds // 60 + 1} min."
                elif check_pw(p, ph):
                    conn.execute(text("UPDATE users SET failed_attempts=0,locked_until=NULL WHERE id=:id"), {"id": uid})
                    st.session_state.update({"logged_in": True, "role": rol, "username": u, "log_err": ""})
                    return
                else:
                    fa += 1
                    if fa >= 6: conn.execute(text("UPDATE users SET is_banned=TRUE,failed_attempts=:f WHERE id=:id"), {"f": fa, "id": uid}); st.session_state.log_err = "❌ Cuenta bloqueada permanentemente."
                    elif fa % 3 == 0: conn.execute(text("UPDATE users SET locked_until=:dt,failed_attempts=:f WHERE id=:id"), {"dt": now_sv + timedelta(minutes=5), "f": fa, "id": uid}); st.session_state.log_err = "⏳ Bloqueado 5 min."
                    else: conn.execute(text("UPDATE users SET failed_attempts=:f WHERE id=:id"), {"f": fa, "id": uid}); st.session_state.log_err = f"❌ Incorrecto. Intento {fa}/6."
            else: st.session_state.log_err = "❌ Usuario o contraseña incorrectos."
    except Exception as e: st.session_state.log_err = f"Error: {e}"
    st.session_state.log_u = ""; st.session_state.log_p = ""

if not st.session_state.logged_in:
    st.markdown("<div style='margin-top:10vh;'></div>", unsafe_allow_html=True)
    _, col_center, _ = st.columns([1, 1.2, 1])
    with col_center:
        if st.session_state.log_msg: st.toast(st.session_state.log_msg, icon="✅"); st.session_state.log_msg = ""
        if st.session_state.log_err: st.error(st.session_state.log_err); st.session_state.log_err = ""
        with st.container(border=True):
            st.markdown("<div style='text-align:center; padding:20px 0 10px 0;'><div style='font-size:46px; line-height:1.1;'>🔐</div><h2 style='margin:10px 0 4px; color:#ffffff; font-weight:700;'>Acceso NOC Central</h2></div>", unsafe_allow_html=True)
            st.text_input("Usuario", key="log_u")
            st.text_input("Contraseña", key="log_p", type="password")
            st.write(""); _, btn_col, _ = st.columns([1, 2, 1])
            with btn_col: st.button("Iniciar Sesión", type="primary", on_click=do_login, use_container_width=True)
            st.write("")
            with st.expander("¿Olvidó su contraseña?"):
                ru = st.text_input("Ingrese su usuario:")
                if ru:
                    try:
                        with engine.connect() as conn:
                            ud = conn.execute(text("SELECT pregunta,respuesta FROM users WHERE username=:u"), {"u": ru}).fetchone()
                        if ud:
                            st.info(f"**Pregunta:** {ud[0]}")
                            rr = st.text_input("Respuesta:", type="password")
                            if rr:
                                if rr.strip().lower() == str(ud[1]).lower():
                                    np_r = st.text_input("Nueva contraseña:", type="password")
                                    if st.button("Actualizar contraseña") and np_r:
                                        with engine.begin() as conn2: conn2.execute(text("UPDATE users SET password_hash=:h, failed_attempts=0, locked_until=NULL, is_banned=FALSE WHERE username=:u"), {"h": hash_pw(np_r), "u": ru})
                                        st.session_state.log_msg = "Restablecida correctamente."; st.rerun()
                                else: st.error("❌ Respuesta incorrecta.")
                        else: st.error("❌ Usuario no encontrado.")
                    except: pass
    st.stop()

# =====================================================================
# SIDEBAR / PANEL LATERAL
# =====================================================================
with st.sidebar:
    st.caption(f"Usuario: **{st.session_state.username}** | NOC v20.0")

    anio_act = datetime.now(SV_TZ).year
    anios    = sorted(list(set([anio_act+1, anio_act, anio_act-1, anio_act-2])), reverse=True)
    a_sel    = st.selectbox("🗓️ Ciclo Anual",   anios, index=anios.index(anio_act))
    m_sel    = st.selectbox("📅 Ciclo Mensual", MESES, index=datetime.now(SV_TZ).month - 1)
    m_idx    = MESES.index(m_sel) + 1
    
    st.divider()
    z_sel   = st.selectbox("🗺️ Filtrar por Zona", ["Todas"] + ZONAS_SV)
    srv_sel = st.selectbox("🌐 Filtrar por Servicio", ["Todos"] + SERVICIOS)
    seg_sel = st.selectbox("🏢 Filtrar por Segmento", ["Todos"] + CATEGORIAS)

    df_m = enriquecer_y_normalizar(load_data_mes(m_idx, a_sel, include_deleted=False, zona_filtro=z_sel, serv_filtro=srv_sel, seg_filtro=seg_sel))

    st.divider()
    if not df_m.empty:
        kpis_s   = calc_kpis(df_m, a_sel, m_idx)
        pdf_data = generar_pdf(m_sel, a_sel, kpis_s, df_m)
        st.download_button(label="📥 Descargar Reporte PDF", data=pdf_data, file_name=f"Reporte_NOC_{m_sel}_{a_sel}.pdf", mime="application/pdf", use_container_width=True)
    else: st.info("Sin datos para reporte.")

    st.divider()
    if st.button("🚪 Cerrar Sesión", use_container_width=True): log_audit("LOGOUT", "Sesión cerrada."); st.session_state.clear(); st.rerun()

# =====================================================================
# PESTAÑAS PRINCIPALES
# =====================================================================
pestanas_base = ["📊 Dashboard", "🧠 Analítica Inteligente"]
pestanas_admin = ["📝 Ingreso Operativo", "🗂️ Auditoría BD", "👥 Usuarios y Logs"] if st.session_state.role == 'admin' else []
tabs = st.tabs(pestanas_base + pestanas_admin)

# ─────────────────────────────────────────────
# TAB 1 — DASHBOARD
# ─────────────────────────────────────────────
with tabs[0]:
    zona_str = "" if z_sel == "Todas" else f" ({z_sel})"
    st.title(f"Visor de Rendimiento: {m_sel} {a_sel}{zona_str}")

    if df_m.empty:
        st.success(f"🟢 Excelente estado: No hay fallas registradas en los filtros seleccionados.")
    else:
        kpis = calc_kpis(df_m, a_sel, m_idx)

        # BLOQUE 1: RESUMEN GLOBAL (Salud General)
        st.markdown("### 🌟 Resumen Global (La red en general)")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("SLA Global", f"{kpis['global']['sla']:.2f}%", help="Disponibilidad basada en eventos medibles (T1 y T2). Solapamientos de horas en diferentes nodos no se restan doble.")
        c2.metric("Total Eventos", kpis['global']['total_fallas'], help="Cantidad absoluta de fallas externas registradas.")
        c3.metric("MTBF", f"{kpis['global']['mtbf']:.1f} hrs", help="Mean Time Between Failures: Tiempo promedio operativo entre fallas válidas.")
        c4.metric("Impacto Acumulado", f"{kpis['global']['db']/24:.1f} días", help="Total de horas caídas válidas (T1 y T2) divididas entre 24 horas.")
        c5.metric("Falla Mayor", f"{kpis['global']['mh']:.1f} hrs", help="Duración del evento más largo (Pico máximo).")
        
        st.divider()

        # BLOQUE 2: TIER 1 (Exactos)
        st.markdown("### 🥇 Tier 1 - Afectación a Clientes (Datos Exactos)")
        if kpis['t1']['fallas'] == 0:
            st.info("No hay incidentes 100% exactos (con tiempo y clientes) en este periodo.")
        else:
            t1a, t1b, t1c, t1d = st.columns(4)
            t1a.metric("Fallas Medidas", kpis['t1']['fallas'], help="Eventos que poseen tanto duración exacta como cantidad de clientes > 0.")
            t1b.metric("MTTR Real", f"{kpis['t1']['mttr']:.2f} hrs", help="Mean Time To Repair: Promedio de tiempo de resolución solo para eventos con afectación a usuarios.")
            t1c.metric("ACD Real", f"{kpis['t1']['acd']:.2f} hrs", help="Afectación Promedio por Cliente: Sensación de interrupción real experimentada por los usuarios.")
            t1d.metric("Clientes Afectados", f"{kpis['t1']['clientes']:,}", help="Sumatoria de clientes impactados en estos eventos.")
        
        st.divider()

        # BLOQUE 3: TIER 2 Y TIER 3 Y P4 (Segmentos Aislados)
        col_t2, col_t3, col_int = st.columns(3)
        with col_t2:
            st.markdown("### 🥈 Tier 2 - Red (Sin Clientes)")
            if kpis['t2']['fallas'] == 0:
                st.success("Sin registros.")
            else:
                t2a, t2b = st.columns(2)
                t2a.metric("Fallas de Red", kpis['t2']['fallas'], help="Eventos con tiempo exacto pero registrados con 0 clientes afectados.")
                t2b.metric("MTTR (Red)", f"{kpis['t2']['mttr']:.2f} hrs", help="Tiempo de resolución de este segmento aislado.")
                
        with col_t3:
            st.markdown("### 🥉 Tier 3 - Incompletos")
            if kpis['t3']['fallas'] == 0:
                st.success("Sin registros.")
            else:
                t3a, t3b = st.columns(2)
                t3a.metric("Fallas Sin Tiempo", kpis['t3']['fallas'], help="Falta hora de inicio o cierre. Se asumen duración 0. Excluidas matemáticamente del SLA.")
                t3b.metric("Clientes (Est.)", f"{kpis['t3']['clientes_est']:,}", help="Posibles clientes afectados por estas fallas.")
                
        with col_int:
            st.markdown("### 🔧 Internas / Infraestructura")
            if kpis['int']['fallas'] == 0:
                st.success("Sin registros.")
            else:
                t4a, t4b = st.columns(2)
                t4a.metric("Eventos Internos", kpis['int']['fallas'], help="No afectan el SLA de los clientes finales.")
                t4b.metric("MTTR Interno", f"{kpis['int']['mttr']:.2f} hrs", help="Tiempo de resolución interno.")

        st.divider()

        st.markdown("### 🗺️ Análisis Geoespacial y Causas Principales")
        df_map = df_m.copy()
        df_map['lat'] = df_map['zona'].apply(lambda x: COORDS.get(x, COORDS["San Salvador (Central)"])[0])
        df_map['lon'] = df_map['zona'].apply(lambda x: COORDS.get(x, COORDS["San Salvador (Central)"])[1])
        agg = df_map.groupby(['zona_completa','lat','lon']).agg(Horas=('duracion_horas','sum'), Clientes=('clientes_afectados','sum')).reset_index()

        fig_m = px.scatter_mapbox(agg, lat="lat", lon="lon", hover_name="zona_completa", size="Clientes", color="Horas", color_continuous_scale="Inferno", zoom=9, mapbox_style="carto-darkmatter")
        fig_m.add_trace(go.Scattermapbox(mode="lines", lat=[13.4900, 13.5850, 13.6929], lon=[-89.3245, -89.2890, -89.2182], line=dict(width=2, color='rgba(241,92,34,0.7)'), name="Ruta Principal"))
        fig_m.update_layout(margin=dict(l=0,r=0,t=0,b=0))
        st.plotly_chart(fig_m, use_container_width=True)

        st.write("")
        dc = df_m.groupby('causa_raiz').size().reset_index(name='Alertas').sort_values('Alertas', ascending=False)
        fig_pie = px.pie(dc, names='causa_raiz', values='Alertas', hole=0.42, color_discrete_sequence=PALETA_CORP)
        fig_pie.update_traces(textinfo='percent+label', hoverinfo='label+value')
        fig_pie.update_layout(showlegend=True, legend=dict(orientation="v", x=1.02), margin=dict(l=0,r=0,t=0,b=0), paper_bgcolor="rgba(0,0,0,0)")
        _, c_pie, _ = st.columns([1, 2, 1])
        with c_pie: sel_p = st.plotly_chart(fig_pie, use_container_width=True, on_select="rerun", selection_mode="points")
        if sel_p and sel_p.selection.point_indices:
            cx = dc.iloc[sel_p.selection.point_indices[0]]['causa_raiz']
            st.info(f"🔍 **Detalle causa:** {cx}")
            st.dataframe(df_m[df_m['causa_raiz'] == cx][['inicio_incidente','zona_completa','Severidad','duracion_horas','descripcion']], hide_index=True)

        st.divider()
        st.markdown("### 📊 Desglose de Equipos y Línea de Tiempo")
        de = df_m.groupby('equipo_afectado').size().reset_index(name='Fallos').sort_values('Fallos')
        fe = px.bar(de, x='Fallos', y='equipo_afectado', orientation='h', color_discrete_sequence=['#f15c22'], text_auto='.0f')
        fe.update_traces(textposition='outside'); fe.update_layout(margin=dict(l=0,r=0,t=30,b=0), paper_bgcolor="rgba(0,0,0,0)", xaxis_title="", yaxis_title="")
        st.plotly_chart(fe, use_container_width=True)

        dg = df_m[df_m['conocimiento_tiempos'] == 'Total'].copy()
        if not dg.empty:
            fg = px.timeline(dg, x_start="inicio_incidente", x_end="fin_incidente", y="zona_completa", color="Severidad", color_discrete_sequence=PALETA_CORP, title="Gantt de Fallas Simultáneas (T1 y T2)")
            fg.update_yaxes(autorange="reversed"); fg.update_traces(marker_line_width=1, marker_line_color="rgba(255,255,255,0.5)")
            fg.update_layout(margin=dict(l=0,r=0,t=40,b=0), paper_bgcolor="rgba(0,0,0,0)"); st.plotly_chart(fg, use_container_width=True)

# ─────────────────────────────────────────────
# TAB 2 — ANALÍTICA INTELIGENTE
# ─────────────────────────────────────────────
with tabs[1]:
    st.title("🧠 Analítica Inteligente")
    if df_m.empty: st.info("Insuficientes datos para analítica.")
    else:
        st.markdown("#### 📈 Panel de Confianza de Datos")
        tot_regs = len(df_m)
        c_completos = df_m[df_m['conocimiento_tiempos'] == 'Total'].shape[0]
        score = int((c_completos / tot_regs) * 100) if tot_regs > 0 else 0
        
        c_q1, c_q2, c_q3, c_q4 = st.columns(4)
        c_q1.metric("Score de Tiempos", f"{score}%", help="Porcentaje de registros que cuentan con tiempos de inicio y fin exactos.")
        c_q2.metric("Registros Íntegros", f"{c_completos} / {tot_regs}")
        c_q3.metric("Fallas P1 Detectadas", f"{len(df_m[df_m['Severidad'] == '🔴 P1 (Crítica)'])}")
        
        # Conteo de faltas de clientes (Tier 2) para el área de analítica
        c_sin_clientes = len(df_m[(df_m['conocimiento_tiempos'] == 'Total') & (df_m['clientes_afectados'] == 0) & (df_m['categoria'] != CAT_INTERNA)])
        c_q4.metric("Faltas de Dato Cliente", f"{c_sin_clientes}", help="Registros que sí tienen tiempo pero el técnico olvidó/omitió poner clientes.")

        st.divider()
        c_cor1, c_cor2 = st.columns(2)
        with c_cor1:
            st.markdown("#### 🛡️ Tasa de Responsabilidad")
            df_resp = df_m.copy()
            df_resp['Tipo_Responsabilidad'] = df_resp['causa_raiz'].apply(lambda x: 'Externa (Fuerza Mayor)' if x in CAUSAS_EXTERNAS else 'Interna (NOC/Infra)')
            df_resp_agg = df_resp.groupby('Tipo_Responsabilidad').size().reset_index(name='Eventos')
            fig_resp = px.pie(df_resp_agg, names='Tipo_Responsabilidad', values='Eventos', hole=0.4, color_discrete_sequence=['#29b09d', '#ff2b2b'])
            fig_resp.update_traces(textinfo='percent+label')
            fig_resp.update_layout(showlegend=False, margin=dict(l=0,r=0,t=0,b=0), paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_resp, use_container_width=True)
            
        with c_cor2:
            st.markdown("#### 🕒 Mapa de Calor por Horarios (Gestión de Turnos)")
            df_time = df_m.copy()
            dias_map = {0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves', 4: 'Viernes', 5: 'Sábado', 6: 'Domingo'}
            dias_orden = list(dias_map.values())
            df_time['Día'] = pd.Categorical(df_time['inicio_incidente'].dt.dayofweek.map(dias_map), categories=dias_orden, ordered=True)
            df_time['Hora'] = df_time['inicio_incidente'].dt.hour
            df_hm_time = df_time.groupby(['Día', 'Hora']).size().reset_index(name='Fallas')
            
            fig_hm_time = px.density_heatmap(df_hm_time, x='Hora', y='Día', z='Fallas', color_continuous_scale='Blues', nbinsx=24)
            fig_hm_time.update_layout(xaxis=dict(tickmode='linear', tick0=0, dtick=1), margin=dict(l=0,r=0,t=0,b=0), paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_hm_time, use_container_width=True)

        st.divider()
        c_cor3, c_cor4 = st.columns(2)
        with c_cor3:
            st.markdown("#### Matriz: Zonas vs. Causas")
            df_heat = df_m.groupby(['zona', 'causa_raiz']).size().reset_index(name='Fallas')
            fig_heat = px.density_heatmap(df_heat, x='causa_raiz', y='zona', z='Fallas', color_continuous_scale='Oranges')
            fig_heat.update_layout(margin=dict(l=0,r=0,t=0,b=0), paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_heat, use_container_width=True)
        with c_cor4:
            st.markdown("#### Dispersión: Duración vs. Clientes (Solo T1)")
            df_t1_disp = df_m[(df_m['conocimiento_tiempos'] == 'Total') & (df_m['clientes_afectados'] > 0)]
            if not df_t1_disp.empty:
                fig_scat = px.scatter(df_t1_disp, x='duracion_horas', y='clientes_afectados', color='Severidad', size='duracion_horas', hover_data=['zona_completa', 'causa_raiz'], color_discrete_sequence=PALETA_CORP)
                fig_scat.update_layout(margin=dict(l=0,r=0,t=0,b=0), paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_scat, use_container_width=True)
            else:
                st.info("Sin datos T1 para graficar dispersión.")

# ─────────────────────────────────────────────
# TABS DE ADMINISTRADOR (3, 4, 5)
# ─────────────────────────────────────────────
if st.session_state.role == 'admin':

    # ── TAB 3: INGRESO OPERATIVO ──
    with tabs[2]:
        st.title("📝 Ingreso Operativo")
        fk = st.session_state.form_reset
        
        try: cmdb_df = pd.read_sql("SELECT zona, subzona, equipo, clientes FROM inventario_nodos", engine)
        except: cmdb_df = pd.DataFrame(columns=['zona', 'subzona', 'equipo', 'clientes'])
        
        cf, ccx = st.columns([2, 1], gap="large")
        with cf:
            with st.container(border=True):
                c_z1, c_z2 = st.columns([1, 1])
                with c_z1: z = st.selectbox("📍 Nodo Principal", ZONAS_SV, key=f"z_{fk}")
                with c_z2:
                    st.markdown("<div style='margin-top: 32px;'></div>", unsafe_allow_html=True)
                    afect_gen = st.toggle("🚨 Falla General (Afecta todo el nodo)", value=True, key=f"ag_{fk}")
                
                if not afect_gen: sz = st.text_input("📍 Especifique Sub-zona (Ej. San Antonio Masahuat)", key=f"sz_{fk}")
                else: sz = "General"
                    
                st.divider()
                c1, c2 = st.columns(2)
                srv = c1.selectbox("🌐 Servicio", SERVICIOS, key=f"s_{fk}")
                cat = c2.selectbox("🏢 Segmento", CATEGORIAS, key=f"c_{fk}")
                eq  = st.selectbox("🖥️ Equipo", EQUIPOS_SV, key=f"e_{fk}")
                st.divider()

                ct1, ct2 = st.columns(2)
                with ct1:
                    fi = st.date_input("📅 Fecha de Inicio", key=f"fi_{fk}")
                    hi_on = st.toggle("🕒 Asignar Hora de Inicio", value=False, key=f"hi_on_{fk}")
                    if hi_on: hi_val = st.time_input("Hora de Apertura", key=f"hi_val_{fk}")
                    else: hi_val = None; st.info("ℹ️ Al no asignar hora de inicio, no se calculará duración.")
                with ct2:
                    ff = st.date_input("📅 Fecha de Cierre", key=f"ff_{fk}")
                    hf_on = st.toggle("🕒 Asignar Hora de Cierre", value=False, key=f"hf_on_{fk}")
                    if hf_on: hf_val = st.time_input("Hora de Cierre", key=f"hf_val_{fk}")
                    else: hf_val = None; st.info("ℹ️ Al no asignar hora de cierre, no se calculará duración.")

                dur = max(0, round((datetime.combine(ff, hf_val) - datetime.combine(fi, hi_val)).total_seconds() / 3600, 2)) if (hi_on and hf_on) else 0
                st.divider()

                cf1, cf2 = st.columns(2)
                with cf1:
                    d_cl = 0
                    if not cmdb_df.empty:
                        m = cmdb_df[(cmdb_df['zona'] == z) & (cmdb_df['subzona'] == sz) & (cmdb_df['equipo'] == eq)]
                        if not m.empty: d_cl = int(m['clientes'].iloc[0])
                    
                    # Tooltip cambia si es corporativo/interno para guiar al técnico
                    cl_f = st.number_input("👤 Clientes Afectados (Deje 0 si no lo sabe)", min_value=0, value=d_cl, step=1, key=f"cl_{fk}")
                    
                    if cat == "Cliente Corporativo": st.caption("🏢 Segmento Corp: Por defecto se recomienda 1 enlace.")
                    elif cat == CAT_INTERNA: st.caption("🔧 Falla Interna: Clientes en 0 por defecto.")
                
                cr = cf2.selectbox("🛠️ Causa Raíz", CAUSAS_RAIZ, key=f"cr_{fk}")
                desc = st.text_area("📝 Descripción del Evento", key=f"desc_{fk}")

                if st.button("💾 Guardar Registro", type="primary"):
                    if (hi_on and hf_on) and (fi > ff or (fi == ff and hi_val > hf_val)):
                        st.toast("❌ Error lógico en fechas.", icon="🚨")
                    else:
                        with st.spinner("Guardando..."):
                            try:
                                hi_db = hi_val if hi_on else datetime_time(0, 0)
                                hf_db = hf_val if hf_on else datetime_time(0, 0)
                                idi = SV_TZ.localize(datetime.combine(fi, hi_db))
                                idf = SV_TZ.localize(datetime.combine(ff, hf_db))
                                
                                with engine.begin() as conn:
                                    conn.execute(text("""
                                        INSERT INTO incidents (zona, subzona, afectacion_general, servicio, categoria, equipo_afectado, inicio_incidente, fin_incidente, clientes_afectados, causa_raiz, descripcion, duracion_horas, conocimiento_tiempos) 
                                        VALUES (:z, :sz, :ag, :s, :c, :e, :idi, :idf, :cl, :cr, :d, :dur, :con)
                                    """), {"z": z, "sz": sz, "ag": afect_gen, "s": srv, "c": cat, "e": eq, "idi": idi, "idf": idf, "cl": cl_f, "cr": cr, "d": desc, "dur": dur, "con": "Total" if (hi_on and hf_on) else "Parcial"})
                                    conn.execute(text("INSERT INTO inventario_nodos (zona, subzona, equipo, clientes) VALUES (:z, :sz, :e, :cl) ON CONFLICT (zona, subzona, equipo) DO UPDATE SET clientes = EXCLUDED.clientes WHERE EXCLUDED.clientes > inventario_nodos.clientes"), {"z": z, "sz": sz, "e": eq, "cl": cl_f})
                                
                                log_audit("INSERT", f"Falla en {z}"); load_data_mes.clear(); st.session_state.form_reset += 1; st.rerun()
                            except Exception as e: st.toast(f"Error: {e}", icon="❌")

        with ccx:
            st.markdown("#### 🕒 Recientes")
            if not df_m.empty:
                for _, r in df_m.sort_values('id', ascending=False).head(5).iterrows():
                    with st.container(border=True):
                        st.markdown(f"**{'🔧' if r.get('categoria')==CAT_INTERNA else '📡'} {r['zona_completa']}**")
                        st.caption(f"{str(r['causa_raiz'])[:22]}... | ⏳ {r['duracion_horas']}h")

    # ── TAB 4: AUDITORÍA BD ──
    with tabs[3]:
        st.markdown("### 🗂️ Auditoría de Base de Datos")
        papelera = st.toggle("🗑️ Ver Papelera de Reciclaje")
        df_audit = load_data_mes(m_idx, a_sel, include_deleted=papelera, zona_filtro="Todas", serv_filtro="Todos", seg_filtro="Todos")
        if df_audit.empty: st.info("No hay datos en esta vista.")
        else:
            c_s, c_pg = st.columns([4, 1])
            bq   = c_s.text_input("🔎 Buscar:", placeholder="Filtrar por campo...")
            df_d = df_audit[df_audit.astype(str).apply(lambda x: x.str.contains(bq, case=False, na=False)).any(axis=1)].copy() if bq else df_audit.copy()

            tot_p = max(1, math.ceil(len(df_d)/15)); pg = c_pg.number_input("Página", 1, tot_p, 1, key="p_bd")
            df_page = df_d.iloc[(pg-1)*15 : pg*15].copy(); df_page.insert(0, "Sel", False)

            drop_cols = [c for c in ['deleted_at', 'Severidad', 'zona_completa', 'data_quality_flag', 'Día', 'Hora'] if c in df_page.columns]
            
            ed_df = st.data_editor(df_page.drop(columns=drop_cols, errors='ignore'), column_config={"Sel": st.column_config.CheckboxColumn("✔", default=False), "id": None, "inicio_incidente": st.column_config.DatetimeColumn("Inicio", format="YYYY-MM-DD HH:mm"), "fin_incidente": st.column_config.DatetimeColumn("Fin", format="YYYY-MM-DD HH:mm")}, use_container_width=True, hide_index=True)

            f_sel  = ed_df[ed_df["Sel"] == True]
            ref_df = df_page.drop(columns=drop_cols + ['Sel'] if 'Sel' in drop_cols else drop_cols, errors='ignore').reset_index(drop=True)
            
            ed_df_c = ed_df.drop(columns=['Sel']).copy()
            if 'inicio_incidente' in ed_df_c: ed_df_c['inicio_incidente'] = pd.to_datetime(ed_df_c['inicio_incidente']).dt.tz_convert(None) if ed_df_c['inicio_incidente'].dt.tz else pd.to_datetime(ed_df_c['inicio_incidente'])
            if 'fin_incidente' in ed_df_c: ed_df_c['fin_incidente'] = pd.to_datetime(ed_df_c['fin_incidente']).dt.tz_convert(None) if ed_df_c['fin_incidente'].dt.tz else pd.to_datetime(ed_df_c['fin_incidente'])
            ref_df_c = ref_df.copy()
            if 'inicio_incidente' in ref_df_c: ref_df_c['inicio_incidente'] = ref_df_c['inicio_incidente'].dt.tz_convert(None) if ref_df_c['inicio_incidente'].dt.tz else ref_df_c['inicio_incidente']
            if 'fin_incidente' in ref_df_c: ref_df_c['fin_incidente'] = ref_df_c['fin_incidente'].dt.tz_convert(None) if ref_df_c['fin_incidente'].dt.tz else ref_df_c['fin_incidente']

            h_cam  = not ref_df_c.equals(ed_df_c)

            cb1, cb2 = st.columns(2)
            if not f_sel.empty:
                if papelera:
                    if cb1.button("♻️ Restaurar Seleccionados", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for rid in f_sel['id']: conn.execute(text("UPDATE incidents SET deleted_at = NULL WHERE id=:id"), {"id": int(rid)})
                        log_audit("RESTORE", f"Restaurados {len(f_sel)} registro(s)."); load_data_mes.clear(); st.rerun()
                else:
                    if cb1.button("🗑️ Eliminar Seleccionados", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for rid in f_sel['id']: conn.execute(text("UPDATE incidents SET deleted_at = CURRENT_TIMESTAMP WHERE id=:id"), {"id": int(rid)})
                        log_audit("DELETE (SOFT)", f"Eliminados {len(f_sel)} registro(s)."); load_data_mes.clear(); st.rerun()

            if h_cam and cb2.button("💾 Guardar Cambios Editados", type="primary", use_container_width=True):
                with engine.begin() as conn:
                    for i, r in ed_df.iterrows():
                        o = ref_df.iloc[i]
                        if not o.equals(r.drop('Sel')):
                            dur = max(0, round((pd.to_datetime(r.get('fin_incidente')) - pd.to_datetime(r.get('inicio_incidente'))).total_seconds() / 3600, 2)) if r.get('conocimiento_tiempos') == 'Total' else 0
                            conn.execute(text("UPDATE incidents SET zona=:z, subzona=:sz, afectacion_general=:ag, servicio=:s, categoria=:c, equipo_afectado=:e, inicio_incidente=:idi, fin_incidente=:idf, clientes_afectados=:cl, causa_raiz=:cr, descripcion=:d, duracion_horas=:dur, conocimiento_tiempos=:con WHERE id=:id"), {"z": r.get('zona',''), "sz": r.get('subzona',''), "ag": bool(r.get('afectacion_general', True)), "s": r.get('servicio',''), "c": r.get('categoria',''), "e": r.get('equipo_afectado',''), "idi": r.get('inicio_incidente', None), "idf": r.get('fin_incidente', None), "cl": int(r.get('clientes_afectados', 0)), "cr": r.get('causa_raiz',''), "d": r.get('descripcion',''), "dur": dur, "con": r.get('conocimiento_tiempos','Total'), "id": int(r['id'])})
                log_audit("UPDATE", "Registros editados."); load_data_mes.clear(); st.rerun()

            st.divider()
            c_e1, c_e2 = st.columns(2)
            with c_e1: st.download_button("📥 Descargar CSV", df_d.drop(columns=drop_cols, errors='ignore').to_csv(index=False).encode(), f"NOC_{m_sel}_{a_sel}.csv", "text/csv", use_container_width=True)

    # ── TAB 5: USUARIOS ──
    with tabs[4]:
        cu, clg = st.columns([1, 2], gap="large")
        with cu:
            st.markdown("#### 👤 Crear Usuario")
            with st.form("form_u", clear_on_submit=True):
                nu, np_u, nrl, npr, nrs = st.text_input("Usuario"), st.text_input("Contraseña", type="password"), st.selectbox("Rol", ["viewer","admin"]), st.text_input("Pregunta de Seguridad"), st.text_input("Respuesta")
                if st.form_submit_button("Crear Usuario") and nu and np_u:
                    try:
                        with engine.begin() as conn: conn.execute(text("INSERT INTO users (username,password_hash,role,pregunta,respuesta) VALUES (:u,:h,:r,:p,:rs)"), {"u": nu, "h": hash_pw(np_u), "r": nrl, "p": npr, "rs": nrs})
                        st.toast("Creado."); time.sleep(1); st.rerun()
                    except: st.toast("Error duplicado.", icon="❌")
        with clg:
            st.markdown("#### 📋 Panel de Usuarios Activos")
            try:
                with engine.connect() as conn: df_usrs = pd.read_sql(text("SELECT id,username,role,is_banned,failed_attempts FROM users"), conn)
                df_usrs.insert(0, "Sel", False)
                ed_usrs = st.data_editor(df_usrs, column_config={"Sel": st.column_config.CheckboxColumn("✔", default=False), "id": None, "username": "Usuario", "role": "Rol", "is_banned": "Baneado", "failed_attempts":"Intentos Fallidos"}, use_container_width=True, hide_index=True)
                filas_del = ed_usrs[ed_usrs["Sel"] == True]
                if not filas_del.empty and st.button("🗑️ Eliminar Usuarios", type="primary"):
                    if "Admin" in filas_del['username'].values: st.error("No puedes eliminar a Admin.")
                    else:
                        with engine.begin() as conn:
                            for rid in filas_del['id']: conn.execute(text("DELETE FROM users WHERE id=:id"), {"id": int(rid)})
                        st.rerun()
            except: pass
