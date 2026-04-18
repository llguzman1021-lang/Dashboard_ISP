import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time, bcrypt, math, pytz, calendar
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
    div.stButton > button:focus { border: none !important; outline: none !important; box-shadow: none !important; }
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
    button[data-baseweb="tab"] p { font-size: 20px !important; font-weight: 700 !important; color: #a5a8b5 !important; margin: 0 !important; }
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
        conn.execute(text("""
            ALTER TABLE users
                ADD COLUMN IF NOT EXISTS failed_attempts INT DEFAULT 0,
                ADD COLUMN IF NOT EXISTS locked_until   TIMESTAMP,
                ADD COLUMN IF NOT EXISTS is_banned      BOOLEAN DEFAULT FALSE
        """))
        if conn.execute(text("SELECT count(*) FROM users WHERE username='Admin'")).scalar() == 0:
            conn.execute(text("INSERT INTO users (username,password_hash,role,pregunta,respuesta) VALUES ('Admin',:h,'admin','¿Color favorito?','azul')"), {"h": hash_pw("Areakde5")})
        if conn.execute(text("SELECT count(*) FROM users WHERE username='viewer'")).scalar() == 0:
            conn.execute(text("INSERT INTO users (username,password_hash,role,pregunta,respuesta) VALUES ('viewer',:h,'viewer','¿Mascota?','perro')"), {"h": hash_pw("view123")})

try:
    init_db()
except Exception as e:
    st.error(f"Error DB: {e}")

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

def normalizar_df(df):
    if df.empty:
        return df
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    df['fecha_convertida']   = pd.to_datetime(df['fecha_inicio'], errors='coerce')
    df['duracion_horas']     = pd.to_numeric(df['duracion_horas'], errors='coerce').fillna(0)
    df['clientes_afectados'] = pd.to_numeric(df['clientes_afectados'], errors='coerce').fillna(0).astype(int)
    if 'categoria' not in df.columns:
        df['categoria'] = 'Red Multinet'
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
    if df.empty:
        return base

    mask_int  = df['categoria'] == CAT_INTERNA
    df_ext    = df[~mask_int].copy()
    df_int    = df[mask_int].copy()

    base["n_internas"]      = len(df_int)
    base["db_int"]          = float(df_int['duracion_horas'].sum())
    base["has_incomplete"]  = df['conocimiento_tiempos'].eq('Parcial').any()

    if not df_int.empty:
        iv = df_int[df_int['duracion_horas'] > 0]
        base["mttr_int"] = float(iv['duracion_horas'].mean()) if not iv.empty else 0.0

    if df_ext.empty:
        return base

    base["db"] = float(df_ext['duracion_horas'].sum())

    # ACD — promedio ponderado, solo externas con clientes > 0
    m_acd = (df_ext['duracion_horas'] > 0) & (df_ext['clientes_afectados'] > 0)
    if m_acd.any():
        base["acd"] = float(
            (df_ext.loc[m_acd, 'duracion_horas'] * df_ext.loc[m_acd, 'clientes_afectados']).sum()
            / df_ext.loc[m_acd, 'clientes_afectados'].sum()
        )

    # SLA — solo externas con tiempos completos, intervalos fusionados
    v_kpi = df_ext[df_ext['conocimiento_tiempos'] == 'Total'].copy()
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

    # MTTR externas
    ev = df_ext[df_ext['duracion_horas'] > 0]
    base["mttr_ext"] = float(ev['duracion_horas'].mean()) if not ev.empty else 0.0

    base["cl"] = int(df_ext['clientes_afectados'].sum())
    mh = df_ext['duracion_horas'].max()
    base["mh"] = float(mh) if pd.notna(mh) else 0.0

    return base

# =====================================================================
# AUDITORÍA
# =====================================================================
def log_audit(action, detail):
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO audit_logs (timestamp,username,action,details) VALUES (:t,:u,:a,:d)"
            ), {"t": datetime.now(SV_TZ).replace(tzinfo=None), "u": st.session_state.username, "a": action, "d": detail})
    except:
        pass

# =====================================================================
# GENERACIÓN DE PDF (ReportLab — corporativo)
# =====================================================================
def generar_pdf(mes, anio, kpis, df):
    RL_ORANGE = rl_colors.HexColor('#f15c22')
    RL_BLUE   = rl_colors.HexColor('#1d2c59')
    RL_TEAL   = rl_colors.HexColor('#29b09d')
    RL_LIGHT  = rl_colors.HexColor('#f5f5f5')
    RL_GRAY   = rl_colors.HexColor('#888888')
    RL_LGRAY  = rl_colors.HexColor('#dddddd')

    def mk_style(name, size, font='Helvetica', color=rl_colors.black, **kw):
        return ParagraphStyle(name, fontSize=size, fontName=font, textColor=color, **kw)

    s_title  = mk_style('t',  22, 'Helvetica-Bold', RL_BLUE,   spaceAfter=2)
    s_sub    = mk_style('s',  11, 'Helvetica',       RL_GRAY,   spaceAfter=14)
    s_period = mk_style('pe', 11, 'Helvetica-Bold',  RL_ORANGE, spaceAfter=2)
    s_body   = mk_style('b',   9, 'Helvetica',       rl_colors.black, spaceAfter=4)
    s_sec    = mk_style('h',  13, 'Helvetica-Bold',  RL_BLUE,   spaceBefore=14, spaceAfter=6)
    s_warn   = mk_style('w',   8, 'Helvetica-Oblique', RL_GRAY, spaceAfter=4)
    s_foot   = mk_style('f',   7, 'Helvetica',       RL_GRAY)

    def table_style(header_color, val_col=True):
        base = [
            ('BACKGROUND',    (0, 0), (-1, 0),  header_color),
            ('TEXTCOLOR',     (0, 0), (-1, 0),  rl_colors.white),
            ('FONTNAME',      (0, 0), (-1, 0),  'Helvetica-Bold'),
            ('FONTSIZE',      (0, 0), (-1, 0),  9),
            ('ROWBACKGROUNDS',(0, 1), (-1, -1), [RL_LIGHT, rl_colors.white]),
            ('FONTNAME',      (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE',      (0, 1), (-1, -1), 9),
            ('VALIGN',        (0, 0), (-1, -1), 'MIDDLE'),
            ('GRID',          (0, 0), (-1, -1), 0.4, RL_LGRAY),
            ('ROWHEIGHT',     (0, 0), (-1, -1), 20),
            ('LEFTPADDING',   (0, 0), (-1, -1), 7),
            ('RIGHTPADDING',  (0, 0), (-1, -1), 7),
        ]
        if val_col:
            base += [
                ('TEXTCOLOR',  (1, 1), (1, -1), RL_ORANGE),
                ('FONTNAME',   (1, 1), (1, -1), 'Helvetica-Bold'),
                ('ALIGN',      (1, 0), (1, -1), 'CENTER'),
            ]
        return TableStyle(base)

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
                            leftMargin=2*cm, rightMargin=2*cm,
                            topMargin=2*cm,  bottomMargin=2*cm)
    story = []
    now_str = datetime.now(SV_TZ).strftime('%d/%m/%Y %H:%M')

    # — Portada / Header —
    story.append(Paragraph("MULTINET", s_title))
    story.append(Paragraph("Reporte Ejecutivo · Network Operations Center", s_sub))
    story.append(HRFlowable(width="100%", thickness=2, color=RL_ORANGE, spaceAfter=10))
    story.append(Paragraph(f"<b>Periodo:</b> {mes} {anio}", s_period))
    story.append(Paragraph(f"<b>Generado:</b> {now_str} (hora El Salvador)", s_body))
    story.append(Spacer(1, 0.5*cm))

    # — Sección 1: KPIs —
    story.append(Paragraph("1. Indicadores Clave de Rendimiento", s_sec))
    kpi_rows = [
        ['Indicador', 'Valor', 'Descripción'],
        ['SLA (Disponibilidad)',  f"{kpis['sla']:.2f}%",          'Porcentaje de tiempo operativo del mes'],
        ['MTTR · Clientes',      f"{kpis['mttr_ext']:.2f} hrs",   'Tiempo promedio de resolución (fallas externas)'],
        ['ACD',                  f"{kpis['acd']:.2f} hrs",         'Afectación promedio ponderada por cliente'],
        ['Clientes Afectados',   f"{kpis['cl']:,}",                'Total de usuarios impactados en el mes'],
        ['Impacto Acumulado',    f"{kpis['db']/24:.2f} días",      'Días equivalentes de caída (fallas externas)'],
        ['Falla Crítica',        f"{kpis['mh']:.2f} hrs",          'Duración de la falla individual más larga'],
    ]
    if kpis['mttr_int'] > 0:
        kpi_rows.append(['MTTR · Internas', f"{kpis['mttr_int']:.2f} hrs", 'Tiempo promedio fallas internas (no afectan SLA)'])

    kpi_t = Table(kpi_rows, colWidths=[5*cm, 3.2*cm, 8.8*cm])
    kpi_t.setStyle(table_style(RL_BLUE))
    story.append(kpi_t)

    if kpis['has_incomplete']:
        story.append(Spacer(1, 0.15*cm))
        story.append(Paragraph(
            "⚠  SLA calculado con datos disponibles. Registros con tiempos incompletos fueron excluidos del cálculo.",
            s_warn
        ))
    story.append(Spacer(1, 0.4*cm))

    if not df.empty:
        df_ext_p = df[df['categoria'] != CAT_INTERNA]

        # — Sección 2: Top Zonas —
        story.append(Paragraph("2. Zonas con Mayor Tiempo de Afectación", s_sec))
        top_z = df_ext_p.groupby('zona')['duracion_horas'].sum().nlargest(8).reset_index()
        if not top_z.empty:
            z_rows = [['Zona / Nodo', 'Horas de Caída', 'Días Equiv.']]
            for _, r in top_z.iterrows():
                z_rows.append([str(r['zona']), f"{r['duracion_horas']:.1f} hrs", f"{r['duracion_horas']/24:.2f}"])
            z_t = Table(z_rows, colWidths=[9*cm, 4.5*cm, 3.5*cm])
            zs = table_style(RL_TEAL, val_col=False)
            zs.add('ALIGN', (1, 0), (-1, -1), 'CENTER')
            z_t.setStyle(zs)
            story.append(z_t)
        story.append(Spacer(1, 0.4*cm))

        # — Sección 3: Top Causas —
        story.append(Paragraph("3. Distribución por Causa Raíz", s_sec))
        top_c = df.groupby('causa_raiz').size().reset_index(name='Eventos').sort_values('Eventos', ascending=False).head(8)
        if not top_c.empty:
            tot = top_c['Eventos'].sum()
            c_rows = [['Causa Raíz', 'Eventos', '% del Total']]
            for _, r in top_c.iterrows():
                c_rows.append([str(r['causa_raiz']), str(r['Eventos']), f"{r['Eventos']/tot*100:.1f}%"])
            c_t = Table(c_rows, colWidths=[10.5*cm, 2.5*cm, 4*cm])
            cs = table_style(RL_BLUE, val_col=False)
            cs.add('ALIGN', (1, 0), (-1, -1), 'CENTER')
            c_t.setStyle(cs)
            story.append(c_t)
        story.append(Spacer(1, 0.4*cm))

        # — Sección 4: Fallas Internas (condicional) —
        if kpis['n_internas'] > 0:
            story.append(Paragraph("4. Resumen de Fallas Internas", s_sec))
            i_rows = [
                ['Métrica', 'Valor'],
                ['Cantidad de Fallas Internas', str(kpis['n_internas'])],
                ['Impacto Total',               f"{kpis['db_int']:.1f} hrs  ({kpis['db_int']/24:.2f} días)"],
                ['MTTR Interno',                f"{kpis['mttr_int']:.2f} hrs"],
            ]
            i_t = Table(i_rows, colWidths=[9*cm, 8*cm])
            i_s = table_style(rl_colors.HexColor('#555555'), val_col=False)
            i_s.add('ALIGN', (1, 0), (-1, -1), 'CENTER')
            i_t.setStyle(i_s)
            story.append(i_t)

    # — Footer —
    story.append(Spacer(1, 1*cm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=RL_LGRAY, spaceAfter=6))
    story.append(Paragraph(
        f"MULTINET NOC  ·  Generado el {now_str}  ·  Documento Confidencial",
        s_foot
    ))

    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()

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
        if st.session_state.log_msg:
            st.toast(st.session_state.log_msg, icon="✅")
            st.session_state.log_msg = ""
        if st.session_state.log_err:
            st.error(st.session_state.log_err)
            st.session_state.log_err = ""

        with st.container(border=True):
            st.markdown("""
                <div style='text-align:center; padding:28px 0 16px 0;'>
                    <div style='font-size:46px; line-height:1.1;'>🔐</div>
                    <h2 style='margin:10px 0 4px; color:#ffffff; font-weight:700;
                               letter-spacing:0.4px; font-size:22px;'>
                        Acceso NOC Central
                    </h2>
                    <p style='color:#a5a8b5; font-size:11px; margin:0;
                              letter-spacing:1.5px; text-transform:uppercase;'>
                        Multinet · Network Operations Center
                    </p>
                </div>
            """, unsafe_allow_html=True)

            st.divider()
            st.text_input("Usuario",    key="log_u", placeholder="Ingrese su usuario")
            st.text_input("Contraseña", key="log_p", type="password", placeholder="••••••••")
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
                            ud = conn.execute(text("SELECT pregunta,respuesta FROM users WHERE username=:u"), {"u": ru}).fetchone()
                        if ud:
                            st.info(f"**Pregunta de seguridad:** {ud[0]}")
                            rr = st.text_input("Respuesta:", type="password")
                            if rr:
                                if rr.strip().lower() == str(ud[1]).lower():
                                    np_r = st.text_input("Nueva contraseña:", type="password")
                                    if st.button("Actualizar contraseña") and np_r:
                                        with engine.begin() as conn2:
                                            conn2.execute(text(
                                                "UPDATE users SET password_hash=:h, failed_attempts=0, locked_until=NULL, is_banned=FALSE WHERE username=:u"
                                            ), {"h": hash_pw(np_r), "u": ru})
                                        st.session_state.log_msg = "Contraseña restablecida correctamente."
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
    st.caption(f"Usuario: **{st.session_state.username}** | v13.0")

    anio_act = datetime.now(SV_TZ).year
    anios    = sorted(list(set([anio_act+1, anio_act, anio_act-1, anio_act-2])), reverse=True)
    a_sel    = st.selectbox("🗓️ Ciclo Anual",   anios, index=anios.index(anio_act))
    m_sel    = st.selectbox("📅 Ciclo Mensual", MESES, index=datetime.now(SV_TZ).month - 1)
    m_idx    = MESES.index(m_sel) + 1
    d_mes    = calendar.monthrange(a_sel, m_idx)[1]

    df_m = normalizar_df(load_data_mes(m_idx, a_sel))

    st.divider()
    if not df_m.empty:
        kpis_s   = calc_kpis(df_m, d_mes * 24)
        pdf_data = generar_pdf(m_sel, a_sel, kpis_s, df_m)
        st.download_button(
            label="📥 Descargar Reporte PDF",
            data=pdf_data,
            file_name=f"Reporte_NOC_{m_sel}_{a_sel}.pdf",
            mime="application/pdf",
            use_container_width=True
        )
    else:
        st.info("Sin datos registrados.")

    st.divider()
    if st.button("🚪 Cerrar Sesión", use_container_width=True):
        log_audit("LOGOUT", "Sesión cerrada.")
        st.session_state.clear()
        st.rerun()

# =====================================================================
# PESTAÑAS PRINCIPALES
# =====================================================================
pestanas = ["📊 Dashboard"] + (
    ["📝 Ingreso", "🗂️ Auditoría BD", "👥 Usuarios y Logs"]
    if st.session_state.role == 'admin' else []
)
tabs = st.tabs(pestanas)

# ─────────────────────────────────────────────
# TAB 0 — DASHBOARD
# ─────────────────────────────────────────────
with tabs[0]:
    st.title(f"Visor de Rendimiento: {m_sel} {a_sel}")

    if df_m.empty:
        st.success(f"🟢 Excelente estado: No hay fallas registradas en {m_sel} {a_sel}.")
    else:
        kpis = calc_kpis(df_m, d_mes * 24)

        # Datos mes anterior para deltas
        df_p     = pd.DataFrame()
        kpis_p   = None
        dm = da = ds = dd = None
        if m_idx > 1:
            df_p = normalizar_df(load_data_mes(m_idx - 1, a_sel))
            if not df_p.empty:
                kpis_p = calc_kpis(df_p, calendar.monthrange(a_sel, m_idx - 1)[1] * 24)
                if kpis_p['mttr_ext'] > 0: dm = f"{kpis['mttr_ext'] - kpis_p['mttr_ext']:+.1f} hrs"
                if kpis_p['acd']     > 0: da = f"{kpis['acd']     - kpis_p['acd']:+.1f} hrs"
                if kpis_p['sla']     > 0: ds = f"{kpis['sla']     - kpis_p['sla']:+.2f}%"
                if kpis_p['db']      > 0: dd = f"{(kpis['db'] - kpis_p['db'])/24:+.1f} días"

        # ── KPIs ──
        st.markdown("### 🎯 Indicadores Clave de Rendimiento (KPIs)")

        _, k1, k2, k3, _ = st.columns([0.5, 2, 2, 2, 0.5])
        k1.metric("MTTR · Clientes", f"{kpis['mttr_ext']:.2f} hrs", dm, "inverse",
                  help="Tiempo Promedio de Resolución — Solo fallas que afectan clientes. Excluye fallas internas.")
        k2.metric("Disponibilidad (SLA)", f"{kpis['sla']:.2f}%", ds,
                  help="SLA calculado con intervalos reales fusionados. Excluye fallas internas y registros sin tiempos completos.")
        k3.metric("Afectación Cliente (ACD)", f"{kpis['acd']:.2f} hrs", da, "inverse",
                  help="Promedio ponderado de horas de interrupción por cliente. Solo fallas externas con clientes > 0.")

        st.write("")
        _, k4, k5, k6, _ = st.columns([0.5, 2, 2, 2, 0.5])
        k4.metric("Falla Crítica", f"{kpis['mh']:.2f} hrs",
                  help="Duración de la falla individual más larga del mes (solo fallas externas).")
        k5.metric("Clientes Afectados", f"{kpis['cl']:,}",
                  help="Suma total de usuarios impactados. Estimación base en registros sin dato exacto.")
        k6.metric("Impacto Acumulado", f"{kpis['db']/24:.1f} días", dd, "inverse",
                  help="Total de horas de caída externa / 24. Representa días equivalentes de servicio perdido.")

        # MTTR Interno — condicional
        if kpis['mttr_int'] > 0:
            st.write("")
            _, ki, _ = st.columns([0.5, 2, 3.5])
            ki.metric(
                f"MTTR · Internas ({kpis['n_internas']} eventos)",
                f"{kpis['mttr_int']:.2f} hrs",
                help="Tiempo promedio de resolución de fallas internas. No impacta SLA, ACD ni clientes."
            )

        if kpis['has_incomplete']:
            st.caption("⚠️ **Aviso:** Existen registros con tiempos incompletos (`Parcial`). El SLA refleja solo datos con tiempos completos.")
        st.caption("ℹ️ **Clientes Afectados:** Estimación base cuando no se dispone de dato exacto, para no distorsionar promedios.")
        st.divider()

        # ── Mapa + Pie ──
        st.markdown("### 🗺️ Análisis Geoespacial y Causas Principales")

        df_map      = df_m.copy()
        df_map['lat'] = df_map['zona'].apply(lambda x: COORDS.get(x, COORDS["Servidores Gabriela Mistral"])[0])
        df_map['lon'] = df_map['zona'].apply(lambda x: COORDS.get(x, COORDS["Servidores Gabriela Mistral"])[1])
        agg = df_map.groupby(['zona','lat','lon']).agg(
            Horas=('duracion_horas','sum'), Clientes=('clientes_afectados','sum')
        ).reset_index()

        fig_m = px.scatter_mapbox(agg, lat="lat", lon="lon", hover_name="zona",
                                  size="Clientes", color="Horas",
                                  color_continuous_scale="Inferno", zoom=9,
                                  mapbox_style="carto-darkmatter")
        fig_m.add_trace(go.Scattermapbox(
            mode="lines",
            lat=[13.5850, 13.4900, 13.3039], lon=[-89.2890, -89.3245, -88.9450],
            line=dict(width=2, color='rgba(241,92,34,0.7)'), name="Troncal Sur"
        ))
        fig_m.update_layout(margin=dict(l=0,r=0,t=0,b=0))
        st.plotly_chart(fig_m, use_container_width=True)

        st.write("")
        CC = {
            "Corte de Fibra por Terceros":        "Terceros",
            "Corte de Fibra (No Especificado)":   "Fibra",
            "Caída de Árboles sobre Fibra":       "Árboles",
            "Falla de Energía Comercial":         "Energía",
            "Corrosión en Equipos":               "Corrosión",
            "Saturación en Servidor UNIFI":       "Sat. UNIFI",
            "Falla de Inicio en UNIFI":           "Inic. UNIFI",
            "Condiciones Climáticas":             "Clima",
        }
        dc = df_m.groupby('causa_raiz').size().reset_index(name='Alertas').sort_values('Alertas', ascending=False)
        dc['Causa'] = dc['causa_raiz'].map(lambda x: CC.get(x, str(x).split()[0]))
        fig_pie = px.pie(dc, names='Causa', values='Alertas', hole=0.42,
                         color_discrete_sequence=PALETA_CORP)
        fig_pie.update_traces(textinfo='percent+label', hoverinfo='label+value')
        fig_pie.update_layout(
            showlegend=True,
            legend=dict(orientation="v", x=1.02),
            margin=dict(l=0,r=0,t=0,b=0),
            paper_bgcolor="rgba(0,0,0,0)"
        )
        _, c_pie, _ = st.columns([1, 2, 1])
        with c_pie:
            sel_p = st.plotly_chart(fig_pie, use_container_width=True,
                                    on_select="rerun", selection_mode="points")
        if sel_p and sel_p.selection.point_indices:
            cx = dc.iloc[sel_p.selection.point_indices[0]]['Causa']
            st.info(f"🔍 **Detalle causa:** {cx}")
            st.dataframe(
                df_m[df_m['causa_raiz'].map(lambda x: CC.get(x, str(x).split()[0])) == cx]
                   [['fecha_inicio','zona','equipo_afectado','duracion_horas','descripcion']],
                hide_index=True
            )

        st.divider()

        # ── Barras Equipo / Servicio ──
        st.markdown("### 📊 Desglose de Afectaciones por Equipo y Servicio")

        de = df_m.groupby('equipo_afectado').size().reset_index(name='Fallos').sort_values('Fallos')
        fe = px.bar(de, x='Fallos', y='equipo_afectado', orientation='h',
                    color_discrete_sequence=['#f15c22'], text_auto='.0f')
        fe.update_traces(textposition='outside')
        fe.update_layout(margin=dict(l=0,r=0,t=30,b=0), paper_bgcolor="rgba(0,0,0,0)",
                         xaxis_title="", yaxis_title="")
        st.plotly_chart(fe, use_container_width=True)

        df_svc = df_m.copy()
        df_svc['Servicio_Label'] = df_svc.apply(
            lambda r: f"{r['servicio']} [Interna]" if r['categoria'] == CAT_INTERNA else r['servicio'],
            axis=1
        )
        dsc = df_svc.groupby('Servicio_Label').size().reset_index(name='Eventos').sort_values('Eventos')
        fs = px.bar(dsc, x='Eventos', y='Servicio_Label', orientation='h',
                    color='Servicio_Label', color_discrete_sequence=PALETA_CORP, text_auto='.0f')
        fs.update_traces(textposition='outside')
        fs.update_layout(showlegend=False, margin=dict(l=0,r=0,t=30,b=0),
                         paper_bgcolor="rgba(0,0,0,0)", xaxis_title="", yaxis_title="")
        st.plotly_chart(fs, use_container_width=True)

        st.divider()

        # ── Tendencia Temporal ──
        st.markdown("### 📈 Análisis Temporal")

        dt_trend = df_m.copy()
        dt_trend['Dia']  = pd.to_datetime(dt_trend['fecha_convertida']).dt.day
        dt_trend['Tipo'] = dt_trend['categoria'].apply(
            lambda x: 'Actual (Interna)' if x == CAT_INTERNA else 'Actual (Externa)'
        )
        da_t = dt_trend.groupby(['Dia','Tipo']).size().reset_index(name='Eventos')
        da_t = da_t.rename(columns={'Tipo': 'Mes'})

        if not df_p.empty:
            dp2 = df_p.copy()
            dp2['Dia'] = pd.to_datetime(dp2['fecha_inicio'], errors='coerce').dt.day
            dpa = dp2.groupby('Dia').size().reset_index(name='Eventos')
            dpa['Mes'] = 'Mes Anterior'
            da_t = pd.concat([da_t, dpa], ignore_index=True)

        ft = px.line(da_t, x='Dia', y='Eventos', color='Mes', markers=True,
                     color_discrete_map={
                         "Actual (Externa)": "#f15c22",
                         "Actual (Interna)": "#83c9ff",
                         "Mes Anterior":     "rgba(255,255,255,0.3)"
                     })
        ft.update_layout(margin=dict(l=0,r=0,t=30,b=0), paper_bgcolor="rgba(0,0,0,0)",
                         xaxis_title="Día del mes", yaxis_title="Fallas")
        st.plotly_chart(ft, use_container_width=True)

        # Gantt
        dg = df_m.dropna(subset=['fecha_inicio','hora_inicio','fecha_fin','hora_fin']).copy()
        if not dg.empty:
            dg['Start'] = pd.to_datetime(dg['fecha_inicio'].astype(str) + ' ' + dg['hora_inicio'].astype(str))
            dg['End']   = pd.to_datetime(dg['fecha_fin'].astype(str)   + ' ' + dg['hora_fin'].astype(str))
            dg['Zona_Label'] = dg.apply(
                lambda r: f"{r['zona']}  🔧 Int." if r['categoria'] == CAT_INTERNA else r['zona'],
                axis=1
            )
            fg = px.timeline(dg, x_start="Start", x_end="End", y="Zona_Label",
                             color="causa_raiz", color_discrete_sequence=PALETA_CORP,
                             title="Gantt de Fallas Simultáneas")
            fg.update_yaxes(autorange="reversed")
            fg.update_traces(marker_line_width=1, marker_line_color="rgba(255,255,255,0.5)")
            fg.update_layout(margin=dict(l=0,r=0,t=40,b=0), paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fg, use_container_width=True)

# ─────────────────────────────────────────────
# TABS DE ADMINISTRADOR
# ─────────────────────────────────────────────
if st.session_state.role == 'admin':

    # ── TAB 1: INGRESO ──
    with tabs[1]:
        st.title("📝 Ingreso Operativo")
        cf, ccx = st.columns([2, 1], gap="large")

        with cf:
            with st.container(border=True):
                z = st.text_input("📍 Nodo o Zona")
                c1, c2 = st.columns(2)
                srv = c1.selectbox("🌐 Servicio", ["Internet", "Cable TV", "IPTV"])
                cat = c2.selectbox("🏢 Segmento", CATEGORIAS)
                eq  = st.selectbox("🖥️ Equipo", ["OLT","RB/Mikrotik","Switch","ONU","Servidor","Fibra Principal","Caja NAP","Sistema UNIFI","Antenas"])
                st.divider()

                ct1, ct2 = st.columns(2)
                with ct1:
                    fi     = st.date_input("📅 Inicio")
                    hi_on  = st.toggle("🕒 Hora Inicio")
                    hi_val = st.time_input("Hora Apertura") if hi_on else None
                    if not hi_val: st.info("ℹ️ Sin hora inicio — duración = 0.")
                with ct2:
                    ff     = st.date_input("📅 Cierre")
                    hf_on  = st.toggle("🕒 Hora Cierre")
                    hf_val = st.time_input("Hora Cierre") if hf_on else None
                    if not hf_val: st.info("ℹ️ Sin hora cierre — duración = 0.")

                dur = (
                    max(0, round(
                        (datetime.combine(ff, hf_val) - datetime.combine(fi, hi_val)).total_seconds() / 3600, 2
                    )) if hi_val and hf_val else 0
                )
                st.divider()

                cf1, cf2 = st.columns(2)
                with cf1:
                    if cat == "Cliente Corporativo":
                        cl_f = 1
                        st.info("🏢 Corporativo: 1 enlace registrado automáticamente.", icon="ℹ️")
                    elif cat == CAT_INTERNA:
                        cl_f = 0
                        st.info("🔧 Falla interna: 0 clientes. No afecta SLA ni ACD.", icon="ℹ️")
                    else:
                        cl_f = st.number_input("👤 Clientes Afectados", min_value=0, step=1)

                cr   = cf2.selectbox("🛠️ Causa Raíz", CAUSAS_RAIZ)
                desc = st.text_area("📝 Descripción del Evento")

                if st.button("💾 Guardar Registro", type="primary"):
                    if fi > ff or (fi == ff and hi_val and hf_val and hi_val > hf_val):
                        st.toast("❌ Error lógico: la fecha/hora de cierre es anterior al inicio.", icon="🚨")
                    else:
                        with st.spinner("Guardando..."):
                            try:
                                with engine.begin() as conn:
                                    conn.execute(text("""
                                        INSERT INTO incidents
                                            (zona,servicio,categoria,equipo_afectado,
                                             fecha_inicio,hora_inicio,fecha_fin,hora_fin,
                                             clientes_afectados,causa_raiz,descripcion,
                                             duracion_horas,conocimiento_tiempos)
                                        VALUES
                                            (:z,:s,:c,:e,:fi,:hi,:ff,:hf,:cl,:cr,:d,:dur,:con)
                                    """), {
                                        "z": z, "s": srv, "c": cat, "e": eq,
                                        "fi": fi.strftime("%Y-%m-%d"),
                                        "hi": hi_val.strftime("%H:%M:%S") if hi_val else None,
                                        "ff": ff.strftime("%Y-%m-%d"),
                                        "hf": hf_val.strftime("%H:%M:%S") if hf_val else None,
                                        "cl": cl_f, "cr": cr, "d": desc, "dur": dur,
                                        "con": "Total" if hi_val and hf_val else "Parcial"
                                    })
                                log_audit("INSERT", f"Falla en {z} [{cat}]")
                                load_data_mes.clear()
                                st.toast("✅ Registro guardado exitosamente.", icon="🎉")
                                time.sleep(1); st.rerun()
                            except Exception as e:
                                st.toast(f"Error al guardar: {e}", icon="❌")

        with ccx:
            st.markdown("#### 🕒 Últimos Registros")
            if not df_m.empty:
                for _, r in df_m.sort_values('id', ascending=False).head(5).iterrows():
                    with st.container(border=True):
                        icono = "🔧" if r.get('categoria') == CAT_INTERNA else "📡"
                        st.markdown(f"**{icono} {r['zona']}**")
                        st.caption(f"{str(r['causa_raiz'])[:22]}... | ⏳ {r['duracion_horas']}h")

    # ── TAB 2: AUDITORÍA BD ──
    with tabs[2]:
        st.markdown("### 🗂️ Auditoría de Base de Datos")
        if df_m.empty:
            st.info("No hay datos registrados para este periodo.")
        else:
            c_s, c_pg = st.columns([4, 1])
            bq   = c_s.text_input("🔎 Buscar:", placeholder="Filtrar por cualquier campo...")
            df_d = (
                df_m[df_m.astype(str).apply(lambda x: x.str.contains(bq, case=False, na=False)).any(axis=1)].copy()
                if bq else df_m.copy()
            )

            tot_p = max(1, math.ceil(len(df_d) / 15))
            pg    = c_pg.number_input("Página", 1, tot_p, 1, key="p_bd")

            df_page = df_d.iloc[(pg-1)*15 : pg*15].copy()
            df_page.insert(0, "Sel", False)

            drop_cols = [c for c in ['fecha_convertida','mes_nombre','lat','lon'] if c in df_page.columns]
            ed_df = st.data_editor(
                df_page.drop(columns=drop_cols, errors='ignore'),
                column_config={
                    "Sel":            st.column_config.CheckboxColumn("✔", default=False),
                    "id":             None,
                    "worksheet_name": None,
                    "gsheet_id":      None,
                },
                use_container_width=True, hide_index=True
            )

            f_del  = ed_df[ed_df["Sel"] == True]
            ref_df = df_page.drop(columns=drop_cols + ['Sel'] if 'Sel' in drop_cols else drop_cols, errors='ignore').reset_index(drop=True)
            h_cam  = not ref_df.equals(ed_df.drop(columns=['Sel']).reset_index(drop=True))

            if not f_del.empty or h_cam:
                cb1, cb2 = st.columns(2)
                if not f_del.empty and cb1.button("🗑️ Eliminar Seleccionados", type="primary", use_container_width=True):
                    with engine.begin() as conn:
                        for rid in f_del['id']:
                            conn.execute(text("DELETE FROM incidents WHERE id=:id"), {"id": int(rid)})
                    log_audit("DELETE", f"Eliminados {len(f_del)} registro(s).")
                    load_data_mes.clear()
                    st.toast("Eliminados correctamente.", icon="✅"); time.sleep(1); st.rerun()

                if h_cam and cb2.button("💾 Guardar Cambios", type="primary", use_container_width=True):
                    with engine.begin() as conn:
                        for i, r in ed_df.iterrows():
                            o = ref_df.iloc[i]
                            if not o.equals(r.drop('Sel')):
                                conn.execute(text("""
                                    UPDATE incidents SET
                                        zona=:z, servicio=:s, categoria=:c, equipo_afectado=:e,
                                        fecha_inicio=:fi, hora_inicio=:hi, fecha_fin=:ff, hora_fin=:hf,
                                        clientes_afectados=:cl, causa_raiz=:cr, descripcion=:d,
                                        duracion_horas=:dur
                                    WHERE id=:id
                                """), {
                                    "z":   r.get('zona',''),
                                    "s":   r.get('servicio',''),
                                    "c":   r.get('categoria',''),
                                    "e":   r.get('equipo_afectado',''),
                                    "fi":  r.get('fecha_inicio',''),
                                    "hi":  None if pd.isna(r.get('hora_inicio')) else r.get('hora_inicio'),
                                    "ff":  r.get('fecha_fin',''),
                                    "hf":  None if pd.isna(r.get('hora_fin')) else r.get('hora_fin'),
                                    "cl":  int(r.get('clientes_afectados', 0)),
                                    "cr":  r.get('causa_raiz',''),
                                    "d":   r.get('descripcion',''),
                                    "dur": float(r.get('duracion_horas', 0)),
                                    "id":  int(r['id'])
                                })
                    log_audit("UPDATE", "Registros editados en auditoría.")
                    load_data_mes.clear()
                    st.toast("Cambios guardados.", icon="✅"); time.sleep(1); st.rerun()

            st.divider()
            c_e1, c_e2 = st.columns(2)
            with c_e1:
                st.download_button("📥 Descargar CSV", df_d.to_csv(index=False).encode(),
                                   f"NOC_{m_sel}_{a_sel}.csv", "text/csv", use_container_width=True)
            with c_e2:
                kpis_a = calc_kpis(df_m, d_mes * 24)
                st.download_button("📥 Descargar PDF", generar_pdf(m_sel, a_sel, kpis_a, df_m),
                                   f"NOC_{m_sel}_{a_sel}.pdf", "application/pdf", use_container_width=True)

    # ── TAB 3: USUARIOS Y LOGS ──
    with tabs[3]:
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
                        with engine.begin() as conn:
                            conn.execute(text(
                                "INSERT INTO users (username,password_hash,role,pregunta,respuesta) VALUES (:u,:h,:r,:p,:rs)"
                            ), {"u": nu, "h": hash_pw(np_u), "r": nrl, "p": npr, "rs": nrs})
                        st.toast(f"Usuario '{nu}' creado.", icon="✅"); time.sleep(1); st.rerun()
                    except:
                        st.toast("Error: usuario duplicado o datos inválidos.", icon="❌")

        with clg:
            st.markdown("#### 📋 Panel de Usuarios Activos")
            try:
                with engine.connect() as conn:
                    df_usrs = pd.read_sql(text(
                        "SELECT id,username,role,is_banned,failed_attempts FROM users"
                    ), conn)
                df_usrs.insert(0, "Seleccionar", False)
                ed_usrs = st.data_editor(
                    df_usrs,
                    column_config={
                        "Seleccionar":    st.column_config.CheckboxColumn("✔", default=False),
                        "id":             None,
                        "username":       "Usuario",
                        "role":           "Rol",
                        "is_banned":      "Baneado",
                        "failed_attempts":"Intentos Fallidos",
                    },
                    use_container_width=True, hide_index=True
                )
                filas_del   = ed_usrs[ed_usrs["Seleccionar"] == True]
                hay_cambios = not df_usrs.drop(columns=['Seleccionar']).reset_index(drop=True).equals(
                    ed_usrs.drop(columns=['Seleccionar']).reset_index(drop=True)
                )

                if not filas_del.empty or hay_cambios:
                    cu1, cu2 = st.columns(2)
                    if not filas_del.empty and cu1.button("🗑️ Eliminar Usuarios", type="primary", use_container_width=True):
                        if "Admin" in filas_del['username'].values:
                            st.error("❌ No se puede eliminar al usuario Admin.")
                        else:
                            with engine.begin() as conn:
                                for rid in filas_del['id']:
                                    conn.execute(text("DELETE FROM users WHERE id=:id"), {"id": int(rid)})
                            st.toast("Usuarios eliminados.", icon="✅"); time.sleep(1); st.rerun()

                    if hay_cambios and cu2.button("💾 Guardar Permisos", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for i, e_row in ed_usrs.iterrows():
                                o_row = df_usrs.drop(columns=['Seleccionar']).iloc[i]
                                if not o_row.equals(e_row):
                                    conn.execute(text(
                                        "UPDATE users SET role=:r,is_banned=:b,failed_attempts=:f WHERE id=:id"
                                    ), {"r": str(e_row['role']), "b": bool(e_row['is_banned']),
                                        "f": int(e_row['failed_attempts']), "id": int(e_row['id'])})
                        st.toast("Permisos actualizados.", icon="✅"); time.sleep(1); st.rerun()
            except Exception as e:
                st.info(f"Error cargando usuarios: {e}")

            st.divider()
            c_lt, c_lp = st.columns([4, 1])
            c_lt.markdown("#### 📜 Registro de Actividad (Audit Log)")
            try:
                with engine.connect() as conn:
                    logs = pd.read_sql(text(
                        "SELECT timestamp as Fecha, username as Usuario, action as Accion, details as Detalles "
                        "FROM audit_logs ORDER BY id DESC"
                    ), conn)
                t_lp  = max(1, math.ceil(len(logs) / 10))
                p_log = c_lp.number_input("Página", 1, t_lp, 1, key="plg")
                st.dataframe(logs.iloc[(p_log-1)*10 : p_log*10], use_container_width=True, hide_index=True)
            except:
                st.info("Sin logs disponibles.")
