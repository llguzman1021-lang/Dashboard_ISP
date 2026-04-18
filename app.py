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

# Realidad Operativa Multinet
ZONAS_SV = ["El Rosario", "ARG", "Tepezontes", "La Libertad", "El Tunco", "Costa del Sol", "Zacatecoluca", "Zaragoza", "Santiago Nonualco", "Rio Mar", "San Salvador (Central)"]
EQUIPOS_SV = ["ONT", "Repetidor Wi-Fi", "Antena Ubiquiti", "OLT", "Caja NAP", "RB/Mikrotik", "Switch", "Servidor", "Fibra Principal", "Sistema UNIFI"]
CAUSAS_RAIZ = ["Corte de Fibra por Terceros","Corte de Fibra (No Especificado)","Caída de Árboles sobre Fibra","Falla de Energía Comercial","Corrosión en Equipos","Daños por Fauna","Falla de Hardware","Falla de Configuración","Falla de Redundancia","Saturación de Tráfico","Saturación en Servidor UNIFI","Falla de Inicio en UNIFI","Mantenimiento Programado","Vandalismo o Hurto","Condiciones Climáticas"]
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
    [data-testid="stMetricLabel"] { color: #a5a8b5 !important; font-size: 16px !important; font-weight: 500 !important; }
    div[data-testid="stTabs"] { background-color: transparent; }
    button[data-baseweb="tab"] { background-color: #1e1e2f !important; border-radius: 12px 12px 0 0 !important; margin-right: 10px !important; padding: 16px 32px !important; border: 2px solid #333 !important; border-bottom: none !important; transition: all 0.3s; }
    button[data-baseweb="tab"]:hover { background-color: #2a2a3f !important; }
    button[data-baseweb="tab"][aria-selected="true"] { background-color: #f15c22 !important; border-color: #f15c22 !important; }
    button[data-baseweb="tab"] p { font-size: 20px !important; font-weight: 700 !important; color: #a5a8b5 !important; margin: 0 !important; }
    button[data-baseweb="tab"][aria-selected="true"] p { color: #ffffff !important; }
    </style>
""", unsafe_allow_html=True)

# =====================================================================
# BASE DE DATOS: DESTRUCCIÓN controlada y REESTRUCTURACIÓN MASIVA
# =====================================================================
@st.cache_resource
def get_engine(): return create_engine(st.secrets["neon_dsn"], pool_pre_ping=True, pool_recycle=300)
engine = get_engine()

def hash_pw(p):  return bcrypt.hashpw(p.encode(), bcrypt.gensalt()).decode()
def check_pw(p, h): return bcrypt.checkpw(p.encode(), h.encode())

def init_db():
    with engine.begin() as conn:
        # 1. Conservar y verificar tabla de usuarios y logs
        conn.execute(text("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, username VARCHAR(50) UNIQUE, password_hash VARCHAR(255), role VARCHAR(20), pregunta VARCHAR(255), respuesta VARCHAR(255), failed_attempts INT DEFAULT 0, locked_until TIMESTAMP, is_banned BOOLEAN DEFAULT FALSE);"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS audit_logs (id SERIAL PRIMARY KEY, timestamp TIMESTAMP, username VARCHAR(50), action VARCHAR(50), details TEXT);"))
        
        # 2. EL HARD RESET (Eliminar tablas operativas viejas si no tienen la columna 'subzona')
        try:
            check = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='incidents' AND column_name='subzona'")).fetchall()
            if len(check) == 0:
                conn.execute(text("DROP TABLE IF EXISTS incidents CASCADE;"))
                conn.execute(text("DROP TABLE IF EXISTS incidents_history CASCADE;"))
                conn.execute(text("DROP TABLE IF EXISTS inventario_nodos CASCADE;"))
        except: pass

        # 3. Creación de la Estructura Empresarial Definitiva
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
                deleted_at TIMESTAMPTZ DEFAULT NULL
            )
        """))
        conn.execute(text("CREATE TABLE IF NOT EXISTS incidents_history (id SERIAL PRIMARY KEY, incident_id INT, changed_by VARCHAR(50), changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, old_data TEXT, new_data TEXT)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS inventario_nodos (id SERIAL PRIMARY KEY, zona VARCHAR(100), subzona VARCHAR(150), equipo VARCHAR(100), clientes INT DEFAULT 0, UNIQUE(zona, subzona, equipo));"))

        # Usuarios Base
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
# LÓGICA DE NEGOCIO: ENRIQUECIMIENTO Y KPIs
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
    df['data_quality_flag'] = df['inicio_incidente'].notna() & df['fin_incidente'].notna()
    
    def eval_severidad(r):
        if r['categoria'] == CAT_INTERNA: return '🟢 P4 (Interna)'
        if r['duracion_horas'] >= 12 or r['clientes_afectados'] >= 1000: return '🔴 P1 (Crítica)'
        if r['duracion_horas'] >= 4 or r['clientes_afectados'] >= 300: return '🟠 P2 (Alta)'
        return '🟡 P3 (Media)'

    df['Severidad'] = df.apply(eval_severidad, axis=1)
    # Etiqueta combinada para vistas
    df['zona_completa'] = df.apply(lambda r: f"{r['zona']} (General)" if r['afectacion_general'] else f"{r['zona']} - {r['subzona']}", axis=1)
    return df

def calc_kpis(df, anio, m_idx):
    base = {"db": 0.0, "acd": 0.0, "sla": 100.0, "mttr_ext": 0.0, "mttr_int": 0.0, "cl": 0, "mh": 0.0, "n_internas": 0, "p1_count": 0}
    if df.empty: return base
    
    h_tot = calendar.monthrange(anio, m_idx)[1] * 24
    m_start = SV_TZ.localize(datetime(anio, m_idx, 1, 0, 0, 0))
    m_end = SV_TZ.localize(datetime(anio, m_idx, calendar.monthrange(anio, m_idx)[1], 23, 59, 59))

    df_int = df[df['categoria'] == CAT_INTERNA]; df_ext = df[df['categoria'] != CAT_INTERNA]
    base["n_internas"] = len(df_int)
    base["p1_count"] = len(df[df['Severidad'] == '🔴 P1 (Crítica)'])

    if not df_int.empty:
        iv = df_int[df_int['duracion_horas'] > 0]
        base["mttr_int"] = float(iv['duracion_horas'].mean()) if not iv.empty else 0.0

    if df_ext.empty: return base
    base["db"] = float(df_ext['duracion_horas'].sum())

    m_acd = (df_ext['duracion_horas'] > 0) & (df_ext['clientes_afectados'] > 0)
    if m_acd.any(): base["acd"] = float((df_ext.loc[m_acd, 'duracion_horas'] * df_ext.loc[m_acd, 'clientes_afectados']).sum() / df_ext.loc[m_acd, 'clientes_afectados'].sum())

    v_kpi = df_ext[df_ext['data_quality_flag'] == True].copy()
    t_real = 0.0
    if not v_kpi.empty:
        s_dt = v_kpi['inicio_incidente'].clip(lower=m_start)
        e_dt = v_kpi['fin_incidente'].clip(upper=m_end)
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
    base["mh"] = float(df_ext['duracion_horas'].max()) if pd.notna(df_ext['duracion_horas'].max()) else 0.0
    return base

# =====================================================================
# FUNCIONES AUXILIARES (Auditoría y Reportes)
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
    story.append(Paragraph("1. Indicadores Clave de Rendimiento", s_sec))
    kpi_rows = [['Indicador', 'Valor', 'Descripción'], ['SLA (Disponibilidad)', f"{kpis['sla']:.2f}%", 'Porcentaje de tiempo operativo'], ['MTTR · Clientes', f"{kpis['mttr_ext']:.2f} hrs", 'Promedio de resolución'], ['ACD', f"{kpis['acd']:.2f} hrs", 'Afectación promedio por cliente'], ['Clientes Afectados', f"{kpis['cl']:,}", 'Usuarios impactados'], ['Impacto Acumulado', f"{kpis['db']/24:.2f} días", 'Días de caída equivalentes'], ['Fallas P1 (Críticas)', f"{kpis['p1_count']}", 'Incidentes críticos']]
    kpi_t = Table(kpi_rows, colWidths=[5*cm, 3.2*cm, 8.8*cm]); kpi_t.setStyle(table_style(RL_BLUE)); story.append(kpi_t); story.append(Spacer(1, 0.4*cm))
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
            c_l1, c_l2 = st.columns([1, 4])
            with c_l1:
                try: st.image("logo.png")
                except: st.markdown("<h2>🌐</h2>", unsafe_allow_html=True)
            with c_l2: st.markdown("<div style='display: flex; align-items: center; height: 100%;'><h3 style='margin: 0;'>Acceso NOC Central</h3></div>", unsafe_allow_html=True)
            st.write("")
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
    st.caption(f"Usuario: **{st.session_state.username}** | Producción v18.0")

    anio_act = datetime.now(SV_TZ).year
    anios    = sorted(list(set([anio_act+1, anio_act, anio_act-1, anio_act-2])), reverse=True)
    a_sel    = st.selectbox("🗓️ Ciclo Anual",   anios, index=anios.index(anio_act))
    m_sel    = st.selectbox("📅 Ciclo Mensual", MESES, index=datetime.now(SV_TZ).month - 1)
    m_idx    = MESES.index(m_sel) + 1
    d_mes    = calendar.monthrange(a_sel, m_idx)[1]
    
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
        st.markdown("### 🎯 Indicadores Clave de Rendimiento (KPIs)")
        _, k1, k2, k3, _ = st.columns([0.5, 2, 2, 2, 0.5])
        k1.metric("MTTR · Clientes", "Óptimo ✅", help="Tiempo Promedio de Resolución — Excluye fallas internas.")
        k2.metric("Disponibilidad (SLA)", "100.00%", help="Porcentaje de tiempo operativo del mes sin fallas reportadas.")
        k3.metric("Afectación Cliente (ACD)", "Óptimo ✅", help="Afectación promedio ponderada por cliente.")
        st.write("")
        _, k4, k5, k6, _ = st.columns([0.5, 2, 2, 2, 0.5])
        k4.metric("Falla Crítica (Mayor)", "Óptimo ✅")
        k5.metric("Clientes Afectados", "0")
        k6.metric("Impacto Acumulado", "0.0 días")
        st.divider()
    else:
        kpis = calc_kpis(df_m, a_sel, m_idx)

        df_p = pd.DataFrame(); kpis_p = None; dm = da = ds = dd = None
        if m_idx > 1:
            df_p = enriquecer_y_normalizar(load_data_mes(m_idx - 1, a_sel, include_deleted=False, zona_filtro=z_sel, serv_filtro=srv_sel, seg_filtro=seg_sel))
            if not df_p.empty:
                kpis_p = calc_kpis(df_p, a_sel, m_idx - 1)
                if kpis_p['mttr_ext'] > 0: dm = f"{kpis['mttr_ext'] - kpis_p['mttr_ext']:+.1f} hrs"
                if kpis_p['acd']     > 0: da = f"{kpis['acd']      - kpis_p['acd']:+.1f} hrs"
                if kpis_p['sla']     > 0: ds = f"{kpis['sla']      - kpis_p['sla']:+.2f}%"
                if kpis_p['db']      > 0: dd = f"{(kpis['db'] - kpis_p['db'])/24:+.1f} días"

        st.markdown("### 🎯 Indicadores Clave de Rendimiento (KPIs)")
        _, k1, k2, k3, _ = st.columns([0.5, 2, 2, 2, 0.5])
        k1.metric("MTTR · Clientes", f"{kpis['mttr_ext']:.2f} hrs" if kpis['mttr_ext'] > 0 else "Óptimo ✅", dm, "inverse", help="Tiempo Promedio de Resolución — Excluye fallas internas.")
        k2.metric("Disponibilidad (SLA)", f"{kpis['sla']:.2f}%", ds, help="SLA calculado con intervalos reales fusionados y ajustados al mes.")
        k3.metric("Afectación Cliente (ACD)", f"{kpis['acd']:.2f} hrs" if kpis['acd'] > 0 else "Óptimo ✅", da, "inverse", help="Promedio ponderado de horas de interrupción por cliente.")

        st.write("")
        _, k4, k5, k6, _ = st.columns([0.5, 2, 2, 2, 0.5])
        k4.metric("Falla Crítica (Mayor)", f"{kpis['mh']:.2f} hrs" if kpis['mh'] > 0 else "Óptimo ✅", help="Duración de la falla individual más larga del mes.")
        k5.metric("Clientes Afectados", f"{kpis['cl']:,}", help="Suma total de usuarios impactados. (Estimación base si no hay dato exacto).")
        k6.metric("Impacto Acumulado", f"{kpis['db']/24:.1f} días", dd, "inverse", help="Total de horas de caída externa divididas entre 24.")

        if kpis['n_internas'] > 0 or kpis['p1_count'] > 0:
            st.write("")
            _, ki, kp, _ = st.columns([0.5, 2, 2, 2.5])
            ki.metric(f"MTTR · Internas ({kpis['n_internas']} ev)", f"{kpis['mttr_int']:.2f} hrs", help="Tiempo promedio de resolución de fallas internas. No impacta SLA.")
            kp.metric(f"Eventos P1 (Críticos)", f"{kpis['p1_count']} alertas", delta_color="inverse", help="Incidentes > 12h o > 1000 clientes.")

        with st.expander("ℹ️ ¿Cómo se calculan estos indicadores?"):
            st.markdown("""
            * **SLA:** Total de horas del mes menos horas reales de caída. Si una falla cruza entre meses, el tiempo se divide matemáticamente. Fallas simultáneas se fusionan. Excluye internas.
            * **MTTR:** (Suma de duración) / (Cantidad de fallas).
            * **ACD:** (Duración x Clientes) / Total Clientes. Impacto real sentido por los usuarios.
            """)
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

        dg = df_m[df_m['data_quality_flag'] == True].copy()
        if not dg.empty:
            fg = px.timeline(dg, x_start="inicio_incidente", x_end="fin_incidente", y="zona_completa", color="Severidad", color_discrete_sequence=PALETA_CORP, title="Gantt de Fallas Simultáneas")
            fg.update_yaxes(autorange="reversed"); fg.update_traces(marker_line_width=1, marker_line_color="rgba(255,255,255,0.5)")
            fg.update_layout(margin=dict(l=0,r=0,t=40,b=0), paper_bgcolor="rgba(0,0,0,0)"); st.plotly_chart(fg, use_container_width=True)

# ─────────────────────────────────────────────
# TAB 2 — ANALÍTICA INTELIGENTE
# ─────────────────────────────────────────────
with tabs[1]:
    st.title("🧠 Analítica Inteligente")
    if df_m.empty: st.info("Insuficientes datos para analítica.")
    else:
        st.markdown("#### 📈 Calidad de Datos (Data Quality Score)")
        tot_regs = len(df_m)
        c_completos = df_m['data_quality_flag'].sum()
        score = int((c_completos / tot_regs) * 100)
        st.progress(score / 100)
        
        c_q1, c_q2, c_q3 = st.columns(3)
        c_q1.metric("Score de Confianza", f"{score}%")
        c_q2.metric("Registros Íntegros", f"{c_completos} / {tot_regs}")
        c_q3.metric("Fallas P1 Detectadas", f"{len(df_m[df_m['Severidad'] == '🔴 P1 (Crítica)'])}")

        st.divider()
        c_cor1, c_cor2 = st.columns(2)
        with c_cor1:
            st.markdown("#### Matriz: Zonas vs. Causas")
            df_heat = df_m.groupby(['zona', 'causa_raiz']).size().reset_index(name='Fallas')
            fig_heat = px.density_heatmap(df_heat, x='causa_raiz', y='zona', z='Fallas', color_continuous_scale='Oranges')
            fig_heat.update_layout(margin=dict(l=0,r=0,t=0,b=0), paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_heat, use_container_width=True)
        with c_cor2:
            st.markdown("#### Dispersión: Duración vs. Clientes")
            fig_scat = px.scatter(df_m, x='duracion_horas', y='clientes_afectados', color='Severidad', size='duracion_horas', hover_data=['zona_completa', 'causa_raiz'], color_discrete_sequence=PALETA_CORP)
            fig_scat.update_layout(margin=dict(l=0,r=0,t=0,b=0), paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_scat, use_container_width=True)

# ─────────────────────────────────────────────
# TABS DE ADMINISTRADOR (3, 4, 5)
# ─────────────────────────────────────────────
if st.session_state.role == 'admin':

    # ── TAB 3: INGRESO OPERATIVO (Manual y Masivo) ──
    with tabs[2]:
        st.title("📝 Ingreso Operativo")
        tab_manual, tab_masivo = st.tabs(["Ingreso Manual", "Importación Masiva (Excel/CSV)"])

        # SUBTAB 1: INGRESO MANUAL
        with tab_manual:
            try: cmdb_df = pd.read_sql("SELECT zona, subzona, equipo, clientes FROM inventario_nodos", engine)
            except: cmdb_df = pd.DataFrame(columns=['zona', 'subzona', 'equipo', 'clientes'])
            
            cf, ccx = st.columns([2, 1], gap="large")
            with cf:
                with st.container(border=True):
                    c_z1, c_z2 = st.columns([1, 1])
                    z = c_z1.selectbox("📍 Nodo Principal", ZONAS_SV)
                    afect_gen = c_z2.checkbox("🚨 Falla General (Afecta todo el nodo)", value=True)
                    if not afect_gen:
                        sz = st.text_input("📍 Especifique Sub-zona (Ej. San Antonio Masahuat)")
                    else:
                        sz = "General"
                        
                    c1, c2 = st.columns(2)
                    srv = c1.selectbox("🌐 Servicio", SERVICIOS)
                    cat = c2.selectbox("🏢 Segmento", CATEGORIAS)
                    eq  = st.selectbox("🖥️ Equipo", EQUIPOS_SV)
                    st.divider()

                    ct1, ct2 = st.columns(2)
                    with ct1:
                        fi     = st.date_input("📅 Fecha de Inicio")
                        hi_val = st.time_input("🕒 Hora de Inicio")
                    with ct2:
                        ff     = st.date_input("📅 Fecha de Cierre")
                        hf_val = st.time_input("🕒 Hora de Cierre")

                    dur = max(0, round((datetime.combine(ff, hf_val) - datetime.combine(fi, hi_val)).total_seconds() / 3600, 2))
                    st.divider()

                    cf1, cf2 = st.columns(2)
                    with cf1:
                        if cat == "Cliente Corporativo": cl_f = 1; st.info("🏢 Corp: 1 enlace auto.", icon="ℹ️")
                        elif cat == CAT_INTERNA: cl_f = 0; st.info("🔧 Interna: 0 clientes.", icon="ℹ️")
                        else:
                            default_cl = 0
                            if not cmdb_df.empty:
                                match = cmdb_df[(cmdb_df['zona'] == z) & (cmdb_df['subzona'] == sz) & (cmdb_df['equipo'] == eq)]
                                if not match.empty: default_cl = int(match['clientes'].iloc[0])
                            cl_f = st.number_input("👤 Clientes Afectados", min_value=0, value=default_cl, step=1)
                    cr   = cf2.selectbox("🛠️ Causa Raíz", CAUSAS_RAIZ)
                    desc = st.text_area("📝 Descripción del Evento")

                    if st.button("💾 Guardar Registro Cerrado", type="primary"):
                        if (fi > ff or (fi == ff and hi_val > hf_val)):
                            st.toast("❌ Error lógico: el cierre es anterior al inicio.", icon="🚨")
                        else:
                            with st.spinner("Guardando..."):
                                try:
                                    inicio_dt = SV_TZ.localize(datetime.combine(fi, hi_val))
                                    fin_dt = SV_TZ.localize(datetime.combine(ff, hf_val))
                                    
                                    with engine.begin() as conn:
                                        conn.execute(text("""
                                            INSERT INTO incidents (zona, subzona, afectacion_general, servicio, categoria, equipo_afectado, inicio_incidente, fin_incidente, clientes_afectados, causa_raiz, descripcion, duracion_horas)
                                            VALUES (:z, :sz, :ag, :s, :c, :e, :idi, :idf, :cl, :cr, :d, :dur)
                                        """), {"z": z, "sz": sz, "ag": afect_gen, "s": srv, "c": cat, "e": eq, "idi": inicio_dt, "idf": fin_dt, "cl": cl_f, "cr": cr, "d": desc, "dur": dur})
                                        conn.execute(text("INSERT INTO inventario_nodos (zona, subzona, equipo, clientes) VALUES (:z, :sz, :e, :cl) ON CONFLICT (zona, subzona, equipo) DO UPDATE SET clientes = EXCLUDED.clientes WHERE EXCLUDED.clientes > inventario_nodos.clientes"), {"z": z, "sz": sz, "e": eq, "cl": cl_f})
                                    log_audit("INSERT", f"Falla en {z} - {sz} [{cat}]"); load_data_mes.clear(); st.toast("✅ Registro guardado exitosamente.", icon="🎉"); time.sleep(1); st.rerun()
                                except Exception as e: st.toast(f"Error al guardar: {e}", icon="❌")

            with ccx:
                st.markdown("#### 🕒 Recientes")
                if not df_m.empty:
                    for _, r in df_m.sort_values('id', ascending=False).head(5).iterrows():
                        with st.container(border=True):
                            st.markdown(f"**{'🔧' if r.get('categoria')==CAT_INTERNA else '📡'} {r['zona_completa']}**")
                            st.caption(f"{str(r['causa_raiz'])[:22]}... | ⏳ {r['duracion_horas']}h")

        # SUBTAB 2: IMPORTADOR MASIVO
        with tab_masivo:
            st.markdown("### 📥 Subir Archivo CSV (Desde Google Sheets)")
            st.info("Para importar datos, tu archivo CSV debe tener las siguientes columnas:\n`zona`, `subzona` (ej. General o Nombre exacto), `afectacion_general` (True/False), `servicio`, `categoria`, `equipo_afectado`, `inicio_incidente` (YYYY-MM-DD HH:MM:SS), `fin_incidente` (YYYY-MM-DD HH:MM:SS), `clientes_afectados`, `causa_raiz`, `descripcion`")
            
            uploaded_file = st.file_uploader("Arrastra tu archivo CSV aquí", type=["csv"])
            if uploaded_file is not None:
                try:
                    df_up = pd.read_csv(uploaded_file)
                    st.dataframe(df_up.head(3), use_container_width=True)
                    if st.button("🚀 Iniciar Importación Masiva", type="primary"):
                        with st.spinner("Procesando y guardando datos..."):
                            df_up['inicio_incidente'] = pd.to_datetime(df_up['inicio_incidente'])
                            df_up['fin_incidente'] = pd.to_datetime(df_up['fin_incidente'])
                            df_up['duracion_horas'] = (df_up['fin_incidente'] - df_up['inicio_incidente']).dt.total_seconds() / 3600.0
                            df_up['duracion_horas'] = df_up['duracion_horas'].fillna(0).round(2)
                            
                            df_sql = df_up[['zona', 'subzona', 'afectacion_general', 'servicio', 'categoria', 'equipo_afectado', 'inicio_incidente', 'fin_incidente', 'clientes_afectados', 'causa_raiz', 'descripcion', 'duracion_horas']].copy()
                            
                            with engine.begin() as conn:
                                df_sql.to_sql('incidents', conn, if_exists='append', index=False)
                            log_audit("IMPORT MASIVO", f"Importados {len(df_up)} registros.")
                            load_data_mes.clear(); st.success("✅ Importación completada."); time.sleep(2); st.rerun()
                except Exception as e: st.error(f"Error al leer el archivo. Revisa el formato. Detalles: {e}")

    # ── TAB 4: AUDITORÍA BD Y PAPELERA ──
    with tabs[3]:
        st.markdown("### 🗂️ Auditoría de Base de Datos")
        
        papelera = st.toggle("🗑️ Ver Papelera de Reciclaje (Registros Eliminados)")
        
        df_audit = load_data_mes(m_idx, a_sel, include_deleted=papelera, zona_filtro="Todas", serv_filtro="Todos", seg_filtro="Todos")
        
        if df_audit.empty:
            st.info("No hay datos en esta vista.")
        else:
            c_s, c_pg = st.columns([4, 1])
            bq   = c_s.text_input("🔎 Buscar:", placeholder="Filtrar por cualquier campo...")
            df_d = df_audit[df_audit.astype(str).apply(lambda x: x.str.contains(bq, case=False, na=False)).any(axis=1)].copy() if bq else df_audit.copy()

            tot_p = max(1, math.ceil(len(df_d) / 15)); pg = c_pg.number_input("Página", 1, tot_p, 1, key="p_bd")
            df_page = df_d.iloc[(pg-1)*15 : pg*15].copy(); df_page.insert(0, "Sel", False)

            drop_cols = [c for c in ['deleted_at', 'Severidad', 'Recomendacion_Auto', 'zona_completa'] if c in df_page.columns]
            
            ed_df = st.data_editor(
                df_page.drop(columns=drop_cols, errors='ignore'),
                column_config={"Sel": st.column_config.CheckboxColumn("✔", default=False), "id": None, "inicio_incidente": st.column_config.DatetimeColumn("Inicio", format="YYYY-MM-DD HH:mm"), "fin_incidente": st.column_config.DatetimeColumn("Fin", format="YYYY-MM-DD HH:mm")},
                use_container_width=True, hide_index=True
            )

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
                        log_audit("RESTORE", f"Restaurados {len(f_sel)} registro(s)."); load_data_mes.clear(); st.toast("Restaurados con éxito.", icon="✅"); time.sleep(1); st.rerun()
                else:
                    if cb1.button("🗑️ Eliminar Seleccionados (Mover a Papelera)", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for rid in f_sel['id']: conn.execute(text("UPDATE incidents SET deleted_at = CURRENT_TIMESTAMP WHERE id=:id"), {"id": int(rid)})
                        log_audit("DELETE (SOFT)", f"Eliminados lógicamente {len(f_sel)} registro(s)."); load_data_mes.clear(); st.toast("Movidos a la papelera.", icon="✅"); time.sleep(1); st.rerun()

            if h_cam and cb2.button("💾 Guardar Cambios Editados", type="primary", use_container_width=True):
                with engine.begin() as conn:
                    for i, r in ed_df.iterrows():
                        o = ref_df.iloc[i]
                        if not o.equals(r.drop('Sel')):
                            log_version(int(r['id']), o.to_json(default_handler=str), r.drop('Sel').to_json(default_handler=str))
                            dur = max(0, round((pd.to_datetime(r.get('fin_incidente')) - pd.to_datetime(r.get('inicio_incidente'))).total_seconds() / 3600, 2)) if r.get('fin_incidente') and pd.notna(r.get('fin_incidente')) else 0
                            conn.execute(text("UPDATE incidents SET zona=:z, subzona=:sz, afectacion_general=:ag, servicio=:s, categoria=:c, equipo_afectado=:e, inicio_incidente=:idi, fin_incidente=:idf, clientes_afectados=:cl, causa_raiz=:cr, descripcion=:d, duracion_horas=:dur WHERE id=:id"), {"z": r.get('zona',''), "sz": r.get('subzona',''), "ag": bool(r.get('afectacion_general', True)), "s": r.get('servicio',''), "c": r.get('categoria',''), "e": r.get('equipo_afectado',''), "idi": r.get('inicio_incidente', None), "idf": r.get('fin_incidente', None), "cl": int(r.get('clientes_afectados', 0)), "cr": r.get('causa_raiz',''), "d": r.get('descripcion',''), "dur": dur, "id": int(r['id'])})
                log_audit("UPDATE", "Registros editados."); load_data_mes.clear(); st.toast("Cambios guardados.", icon="✅"); time.sleep(1); st.rerun()

            st.divider()
            c_e1, c_e2 = st.columns(2)
            with c_e1: st.download_button("📥 Descargar CSV Crudo", df_d.drop(columns=drop_cols, errors='ignore').to_csv(index=False).encode(), f"NOC_{m_sel}_{a_sel}.csv", "text/csv", use_container_width=True)

    # ── TAB 5: USUARIOS Y LOGS ──
    with tabs[4]:
        cu, clg = st.columns([1, 2], gap="large")

        with cu:
            st.markdown("#### 👤 Crear Usuario")
            with st.form("form_u", clear_on_submit=True):
                nu   = st.text_input("Usuario")
                np_u = st.text_input("Contraseña", type="password")
                nrl  = st.selectbox("Rol", ["viewer","admin"])
                npr  = st.text_input("Pregunta de Seguridad")
                nrs  = st.text_input("Respuesta")
                if st.form_submit_button("Crear Usuario") and nu and np_u:
                    try:
                        with engine.begin() as conn: conn.execute(text("INSERT INTO users (username,password_hash,role,pregunta,respuesta) VALUES (:u,:h,:r,:p,:rs)"), {"u": nu, "h": hash_pw(np_u), "r": nrl, "p": npr, "rs": nrs})
                        st.toast(f"Usuario '{nu}' creado.", icon="✅"); time.sleep(1); st.rerun()
                    except: st.toast("Error: usuario duplicado.", icon="❌")

        with clg:
            st.markdown("#### 📋 Panel de Usuarios Activos")
            try:
                with engine.connect() as conn: df_usrs = pd.read_sql(text("SELECT id,username,role,is_banned,failed_attempts FROM users"), conn)
                df_usrs.insert(0, "Seleccionar", False)
                ed_usrs = st.data_editor(df_usrs, column_config={"Seleccionar": st.column_config.CheckboxColumn("✔", default=False), "id": None, "username": "Usuario", "role": "Rol", "is_banned": "Baneado", "failed_attempts":"Intentos Fallidos"}, use_container_width=True, hide_index=True)
                filas_del   = ed_usrs[ed_usrs["Seleccionar"] == True]
                hay_cambios = not df_usrs.drop(columns=['Seleccionar']).reset_index(drop=True).equals(ed_usrs.drop(columns=['Seleccionar']).reset_index(drop=True))

                if not filas_del.empty or hay_cambios:
                    cu1, cu2 = st.columns(2)
                    if not filas_del.empty and cu1.button("🗑️ Eliminar Usuarios", type="primary", use_container_width=True):
                        if "Admin" in filas_del['username'].values: st.error("❌ No se puede eliminar a Admin.")
                        else:
                            with engine.begin() as conn:
                                for rid in filas_del['id']: conn.execute(text("DELETE FROM users WHERE id=:id"), {"id": int(rid)})
                            st.toast("Usuarios eliminados.", icon="✅"); time.sleep(1); st.rerun()
                    if hay_cambios and cu2.button("💾 Guardar Permisos", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for i, e_row in ed_usrs.iterrows():
                                o_row = df_usrs.drop(columns=['Seleccionar']).iloc[i]
                                if not o_row.equals(e_row): conn.execute(text("UPDATE users SET role=:r,is_banned=:b,failed_attempts=:f WHERE id=:id"), {"r": str(e_row['role']), "b": bool(e_row['is_banned']), "f": int(e_row['failed_attempts']), "id": int(e_row['id'])})
                        st.toast("Permisos actualizados.", icon="✅"); time.sleep(1); st.rerun()
            except: pass

            st.divider()
            c_lt, c_lp = st.columns([4, 1])
            c_lt.markdown("#### 📜 Registro de Actividad (Audit Log)")
            try:
                with engine.connect() as conn: logs = pd.read_sql(text("SELECT timestamp as Fecha, username as Usuario, action as Accion, details as Detalles FROM audit_logs ORDER BY id DESC"), conn)
                t_lp  = max(1, math.ceil(len(logs) / 10)); p_log = c_lp.number_input("Página", 1, t_lp, 1, key="plg")
                st.dataframe(logs.iloc[(p_log-1)*10 : p_log*10], use_container_width=True, hide_index=True)
            except: pass
