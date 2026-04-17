import streamlit as st, pandas as pd, plotly.express as px, plotly.graph_objects as go, time, bcrypt, math, pytz, calendar
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from fpdf import FPDF

# =====================================================================
# [CONFIGURACIÓN Y ESTILOS GLOBALES]
# =====================================================================
st.set_page_config(page_title="Multinet NOC", layout="wide", page_icon="🌐")
SV_TZ = pytz.timezone('America/El_Salvador')

PALETA_CORP = ['#f15c22', '#1d2c59', '#29b09d', '#ff9f43', '#83c9ff', '#ff2b2b']
MESES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
CAUSAS_RAIZ = ["Corte de Fibra por Terceros", "Corte de Fibra (No Especificado)", "Caída de Árboles sobre Fibra", "Falla de Energía Comercial", "Corrosión en Equipos", "Daños por Fauna", "Falla de Hardware", "Falla de Configuración", "Falla de Redundancia", "Saturación de Tráfico", "Saturación en Servidor UNIFI", "Falla de Inicio en UNIFI", "Mantenimiento Programado", "Vandalismo o Hurto", "Condiciones Climáticas"]
COORDS = {"Papaya Garden": [13.4925, -89.3822], "La Libertad - Conchalio": [13.4900, -89.3245], "La Libertad - Julupe": [13.5011, -89.3300], "Costa del Sol": [13.3039, -88.9450], "OLT ARG": [13.4880, -89.3200], "La Libertad - Agroferreteria": [13.4905, -89.3210], "Servidores Gabriela Mistral": [13.7000, -89.2000], "Los Blancos": [13.3100, -88.9200], "Zaragoza": [13.5850, -89.2890]}

st.markdown("""
    <style>
    /* Efecto hover para botones (Sin sombra naranja, pero con salto) */
    div.stButton > button { border-radius: 8px; width: 100%; font-weight: 600; transition: all 0.3s ease !important; }
    div.stButton > button:hover { transform: translateY(-2px); }
    div[data-testid="stButton-delete"] > button { background-color: #c0392b !important; color: white !important;}
    div[data-testid="stButton-save"] > button { background-color: #27ae60 !important; color: white !important;}
    div.stButton > button:first-child { background-color: #0068c9; color: white; }
    
    [data-testid="stMetricValue"] { color: #ffffff !important; font-size: 36px !important; font-weight: 800 !important; }
    [data-testid="stMetricLabel"] { color: #a5a8b5 !important; font-size: 16px !important; font-weight: 500 !important; }
    div[data-testid="stTabs"] { background-color: transparent; }
    button[data-baseweb="tab"] { background-color: #1e1e2f !important; border-radius: 12px 12px 0px 0px !important; margin-right: 10px !important; padding: 16px 32px !important; border: 2px solid #333 !important; border-bottom: none !important; transition: all 0.3s; }
    button[data-baseweb="tab"]:hover { background-color: #2a2a3f !important; }
    button[data-baseweb="tab"][aria-selected="true"] { background-color: #f15c22 !important; border-color: #f15c22 !important; }
    button[data-baseweb="tab"] p { font-size: 20px !important; font-weight: 700 !important; color: #a5a8b5 !important; margin: 0px !important; }
    button[data-baseweb="tab"][aria-selected="true"] p { color: #ffffff !important; }
    </style>
    """, unsafe_allow_html=True)

# =====================================================================
# [FUNCIONES CORE: BD, ENCRIPTACIÓN Y MÉTRICAS]
# =====================================================================
@st.cache_resource
def get_engine(): return create_engine(st.secrets["neon_dsn"], pool_pre_ping=True, pool_recycle=300)
engine = get_engine()

def hash_pw(p): return bcrypt.hashpw(p.encode(), bcrypt.gensalt()).decode()
def check_pw(p, h): return bcrypt.checkpw(p.encode(), h.encode())

def init_db():
    with engine.begin() as c:
        c.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS failed_attempts INT DEFAULT 0, ADD COLUMN IF NOT EXISTS locked_until TIMESTAMP, ADD COLUMN IF NOT EXISTS is_banned BOOLEAN DEFAULT FALSE;"))
        if c.execute(text("SELECT count(*) FROM users WHERE username = 'Admin'")).scalar() == 0:
            c.execute(text("INSERT INTO users (username, password_hash, role, pregunta, respuesta) VALUES ('Admin', :h, 'admin', '¿Color favorito?', 'azul')"), {"h": hash_pw("Areakde5")})
        if c.execute(text("SELECT count(*) FROM users WHERE username = 'viewer'")).scalar() == 0:
            c.execute(text("INSERT INTO users (username, password_hash, role, pregunta, respuesta) VALUES ('viewer', :h, 'viewer', '¿Mascota?', 'perro')"), {"h": hash_pw("view123")})
try: init_db()
except Exception as e: st.error(f"Error DB: {e}")

@st.cache_data(ttl=300)
def load_data_mes(m_idx, anio):
    end_d = calendar.monthrange(anio, m_idx)[1]
    query = "SELECT * FROM incidents WHERE fecha_inicio >= :s AND fecha_inicio <= :e ORDER BY fecha_inicio ASC"
    try:
        with engine.connect() as conn:
            conn.execute(text("ROLLBACK"))
            return pd.read_sql(text(query), conn, params={"s": f"{anio}-{m_idx:02d}-01", "e": f"{anio}-{m_idx:02d}-{end_d}"})
    except: return pd.DataFrame()

def calc_kpis(df, h_tot):
    if df.empty: return 0.0, 0.0, 100.0, 0.0, 0, 0.0
    db = df['duracion_horas'].sum()
    m_acd = (df['duracion_horas'] > 0) & (df['clientes_afectados'] > 0)
    acd = (df.loc[m_acd, 'duracion_horas'] * df.loc[m_acd, 'clientes_afectados']).sum() / df.loc[m_acd, 'clientes_afectados'].sum() if m_acd.any() else 0.0
    t_real = 0.0
    v_kpi = df[df['conocimiento_tiempos'] == 'Total'].copy()
    if not v_kpi.empty:
        s = pd.to_datetime(v_kpi['fecha_inicio'].astype(str) + ' ' + v_kpi['hora_inicio'].astype(str), errors='coerce')
        e = pd.to_datetime(v_kpi['fecha_fin'].astype(str) + ' ' + v_kpi['hora_fin'].astype(str), errors='coerce')
        m = s.notna() & e.notna() & (s <= e)
        if m.any():
            mg = [list(i) for i in sorted(list(zip(s[m], e[m])), key=lambda x: x[0])]
            res = [mg[0]]
            for c in mg[1:]:
                if c[0] <= res[-1][1]: res[-1][1] = max(res[-1][1], c[1])
                else: res.append(c)
            t_real = sum((end - stp).total_seconds() for stp, end in res) / 3600.0
    sla = max(0.0, min(100.0, ((h_tot - t_real) / h_tot) * 100)) if h_tot > 0 else 100.0
    mttr = df.loc[df['duracion_horas'] > 0, 'duracion_horas'].mean() if not df[df['duracion_horas'] > 0].empty else 0.0
    return db, acd, sla, mttr, int(df['clientes_afectados'].sum()), df['duracion_horas'].max()

def log_audit(act, det):
    try:
        with engine.begin() as c: c.execute(text("INSERT INTO audit_logs (timestamp, username, action, details) VALUES (:t, :u, :a, :d)"), {"t": datetime.now(SV_TZ).replace(tzinfo=None), "u": st.session_state.username, "a": act, "d": det})
    except: pass

def generar_pdf(mes, anio, mttr, sla, acd, cl, d_tot, df):
    p = FPDF()
    p.add_page()
    p.set_font("Arial", 'B', 16); p.cell(200, 10, "MULTINET - REPORTE EJECUTIVO NOC", ln=True, align='C')
    p.set_font("Arial", 'I', 12); p.cell(200, 10, f"Periodo: {mes} {anio}", ln=True, align='C'); p.ln(10)
    p.set_font("Arial", 'B', 14); p.cell(200, 10, "1. Indicadores (KPIs)", ln=True)
    p.set_font("Arial", '', 12)
    p.cell(200, 8, f"- SLA: {sla:.2f}% | MTTR: {mttr:.2f} hrs | ACD: {acd:.2f} hrs | Afectados: {cl} | Impacto: {d_tot:.1f} dias", ln=True); p.ln(10)
    p.set_font("Arial", 'B', 14); p.cell(200, 10, "2. Top 5 Zonas Afectadas", ln=True)
    p.set_font("Arial", '', 12)
    if not df.empty:
        for z, h in df.groupby('zona')['duracion_horas'].sum().nlargest(5).items(): p.cell(200, 8, f"- {z}: {h:.1f} hrs", ln=True)
    try: out = p.output()
    except: out = p.output(dest='S')
    return out.encode('latin-1') if isinstance(out, str) else bytes(out)

# =====================================================================
# [SISTEMA DE LOGIN]
# =====================================================================
for k in ['logged_in', 'role', 'username', 'log_u', 'log_p', 'log_err', 'log_msg']: 
    if k not in st.session_state: st.session_state[k] = False if k == 'logged_in' else ("" if "log" in k else None)

def do_login():
    u, p = st.session_state.log_u, st.session_state.log_p
    try:
        with engine.begin() as c:
            ud = c.execute(text("SELECT id, password_hash, role, failed_attempts, locked_until, is_banned FROM users WHERE username = :u"), {"u": u}).fetchone()
            if ud:
                uid, ph, rol, fa, ldt, ban = ud
                fa = fa or 0; now_sv = datetime.now(SV_TZ).replace(tzinfo=None)
                if ban: st.session_state.log_err = "❌ Cuenta baneada."
                elif ldt and ldt > now_sv: st.session_state.log_err = f"⏳ Bloqueado. Intente en {(ldt - now_sv).seconds // 60 + 1} min."
                elif check_pw(p, ph):
                    c.execute(text("UPDATE users SET failed_attempts=0, locked_until=NULL WHERE id=:id"), {"id": uid})
                    st.session_state.update({"logged_in": True, "role": rol, "username": u, "log_err": ""})
                    return
                else:
                    fa += 1
                    if fa >= 6: c.execute(text("UPDATE users SET is_banned=TRUE, failed_attempts=:f WHERE id=:id"), {"f": fa, "id": uid}); st.session_state.log_err = "❌ Bloqueado permanentemente (6 intentos)."
                    elif fa % 3 == 0: c.execute(text("UPDATE users SET locked_until=:dt, failed_attempts=:f WHERE id=:id"), {"dt": now_sv + timedelta(minutes=5), "f": fa, "id": uid}); st.session_state.log_err = "⏳ Bloqueado 5 min."
                    else: c.execute(text("UPDATE users SET failed_attempts=:f WHERE id=:id"), {"f": fa, "id": uid}); st.session_state.log_err = f"❌ Incorrecto. Intento {fa}/6."
            else: st.session_state.log_err = "❌ Incorrecto."
    except Exception as e: st.session_state.log_err = f"Error: {e}"
    st.session_state.log_u = ""; st.session_state.log_p = ""

if not st.session_state.logged_in:
    st.markdown("<div style='margin-top: 10vh;'></div>", unsafe_allow_html=True)
    _, c2, _ = st.columns([1, 1.2, 1])
    with c2:
        if st.session_state.log_msg: st.toast(st.session_state.log_msg, icon="✅"); st.session_state.log_msg = ""
        if st.session_state.log_err: st.error(st.session_state.log_err); st.session_state.log_err = ""
        
        with st.container(border=True):
            # LOGO PEQUEÑO Y TÍTULO ALINEADOS
            col_l1, col_l2 = st.columns([1, 4])
            with col_l1:
                try: st.image("logo.png")
                except: st.markdown("<h2>🌐</h2>", unsafe_allow_html=True)
            with col_l2:
                st.markdown("<div style='display: flex; align-items: center; height: 100%;'><h3 style='margin: 0;'>Acceso NOC Central</h3></div>", unsafe_allow_html=True)
            
            st.write("")
            st.text_input("Usuario", key="log_u")
            st.text_input("Contraseña", type="password", key="log_p")
            
            # BOTÓN DE LOGIN CENTRADO
            st.write("")
            _, c_btn, _ = st.columns([1, 2, 1])
            with c_btn:
                st.button("Iniciar Sesión", type="primary", on_click=do_login, use_container_width=True)
            
            st.write("")
            with st.expander("¿Olvidó su contraseña?"):
                ru = st.text_input("Ingrese su usuario:")
                if ru:
                    try:
                        with engine.connect() as c:
                            ud = c.execute(text("SELECT pregunta, respuesta FROM users WHERE username=:u"), {"u": ru}).fetchone()
                            if ud:
                                st.info(f"**Pregunta:** {ud[0]}")
                                rr = st.text_input("Respuesta:", type="password")
                                if rr:
                                    if rr.strip().lower() == str(ud[1]).lower():
                                        np = st.text_input("Nueva contraseña:", type="password")
                                        if st.button("Actualizar") and np:
                                            with engine.begin() as c2: c2.execute(text("UPDATE users SET password_hash=:h, failed_attempts=0, locked_until=NULL, is_banned=FALSE WHERE username=:u"), {"h": hash_pw(np), "u": ru})
                                            st.session_state.log_msg = "Contraseña restablecida."; st.rerun()
                                    else: st.error("❌ Respuesta incorrecta.")
                            else: st.error("❌ Usuario no existe.")
                    except: pass
    st.stop()

# =====================================================================
# [SIDEBAR Y EXTRACCIÓN DE DATOS GLOBALES]
# =====================================================================
with st.sidebar:
    st.caption(f"Usuario: **{st.session_state.username}** | v12.3")
    
    anio_act = datetime.now(SV_TZ).year
    anios = sorted(list(set([anio_act+1, anio_act, anio_act-1, anio_act-2])), reverse=True)
    a_sel = st.selectbox("🗓️ Ciclo Anual", anios, index=anios.index(anio_act))
    m_sel = st.selectbox("📅 Ciclo Mensual", MESES, index=datetime.now(SV_TZ).month - 1)
    m_idx = MESES.index(m_sel) + 1
    d_mes = calendar.monthrange(a_sel, m_idx)[1]
    
    df_m = load_data_mes(m_idx, a_sel)
    if not df_m.empty:
        df_m.columns = [c.lower() for c in df_m.columns]
        df_m['fecha_convertida'] = pd.to_datetime(df_m['fecha_inicio'], errors='coerce')
        df_m['duracion_horas'] = pd.to_numeric(df_m['duracion_horas'], errors='coerce').fillna(0)
        df_m['clientes_afectados'] = pd.to_numeric(df_m['clientes_afectados'], errors='coerce').fillna(0).astype(int)
    
    st.divider()
    if not df_m.empty:
        dt, acds, slas, mttrs, cls, mhs = calc_kpis(df_m, d_mes * 24)
        pdf_data = generar_pdf(m_sel, a_sel, mttrs, slas, acds, cls, dt/24.0, df_m)
        st.download_button(label="📥 Descargar Reporte PDF", data=pdf_data, file_name=f"Reporte_NOC_{m_sel}_{a_sel}.pdf", mime="application/pdf", use_container_width=True)
    else: st.info("Sin datos registrados.")
        
    st.divider()
    if st.button("🚪 Cerrar Sesión", use_container_width=True): log_audit("LOGOUT", "Sesión cerrada."); st.session_state.clear(); st.rerun()

# =====================================================================
# [ESTRUCTURA DE PESTAÑAS Y DASHBOARD]
# =====================================================================
pestanas = ["📊 Dashboard"] + (["📝 Ingreso", "🗂️ Auditoría BD", "👥 Usuarios y Logs"] if st.session_state.role == 'admin' else [])
tabs = st.tabs(pestanas)

with tabs[0]:
    st.title(f"Visor de Rendimiento: {m_sel} {a_sel}")
    if df_m.empty: st.success(f"🟢 Excelente estado: No hay fallas registradas en {m_sel} {a_sel}.")
    else:
        dt, acdh, sla, mttr, cl, mh = calc_kpis(df_m, d_mes * 24)
        dm = da = ds = dd = None
        if m_idx > 1:
            df_p = load_data_mes(m_idx - 1, a_sel)
            if not df_p.empty:
                df_p['duracion_horas'] = pd.to_numeric(df_p['duracion_horas'], errors='coerce').fillna(0)
                df_p['clientes_afectados'] = pd.to_numeric(df_p['clientes_afectados'], errors='coerce').fillna(0).astype(int)
                dbp, acdp, slap, mttrp, _, _ = calc_kpis(df_p, calendar.monthrange(a_sel, m_idx - 1)[1] * 24)
                if mttrp > 0: dm = f"{mttr - mttrp:+.1f} hrs"
                if acdp > 0: da = f"{acdh - acdp:+.1f} hrs"
                if slap > 0: ds = f"{sla - slap:+.2f}%"
                if dbp > 0: dd = f"{(dt/24.0) - (dbp/24.0):+.1f} días"

        st.markdown("### 🎯 Indicadores Clave de Rendimiento (KPIs)")
        _, k1, k2, k3, _ = st.columns([0.5, 2, 2, 2, 0.5])
        k1.metric("MTTR", f"{mttr:.2f} hrs", dm, "inverse", help="Tiempo Promedio de Resolución")
        k2.metric("Disponibilidad (SLA)", f"{sla:.2f}%", ds, help="Nivel integral de servicio")
        k3.metric("Afectación Cliente (ACD)", f"{acdh:.2f} hrs", da, "inverse", help="Horas promedio de interrupción")
        
        st.write("")
        _, k4, k5, k6, _ = st.columns([0.5, 2, 2, 2, 0.5])
        k4.metric("Falla Crítica", f"{mh:.2f} hrs")
        k5.metric("Afectados", f"{cl} usuarios")
        k6.metric("Impacto Acumulado", f"{dt/24.0:.1f} días", dd, "inverse")
        st.caption("ℹ️ **Nota sobre Clientes Afectados:** La cantidad mostrada es una estimación base cuando no se cuenta con el dato exacto para no alterar promedios.")
        st.divider()

        st.markdown("### 🗺️ Análisis Geoespacial y Causas Principales")
        df_map = df_m.copy()
        df_map['lat'] = df_map['zona'].apply(lambda x: COORDS.get(x, COORDS["Servidores Gabriela Mistral"])[0])
        df_map['lon'] = df_map['zona'].apply(lambda x: COORDS.get(x, COORDS["Servidores Gabriela Mistral"])[1])
        agg = df_map.groupby(['zona', 'lat', 'lon']).agg(Horas=('duracion_horas', 'sum'), Clientes=('clientes_afectados', 'sum')).reset_index()
        fig_m = px.scatter_mapbox(agg, lat="lat", lon="lon", hover_name="zona", size="Clientes", color="Horas", color_continuous_scale="Inferno", zoom=9, mapbox_style="carto-darkmatter")
        fig_m.add_trace(go.Scattermapbox(mode="lines", lat=[13.5850, 13.4900, 13.3039], lon=[-89.2890, -89.3245, -88.9450], line=dict(width=2, color='rgba(241, 92, 34, 0.7)'), name="Troncal Sur"))
        fig_m.update_layout(margin=dict(l=0, r=0, t=0, b=0)); st.plotly_chart(fig_m, use_container_width=True)

        st.write("")
        cc = {"Corte de Fibra por Terceros": "Terceros", "Corte de Fibra (No Especificado)": "Fibra", "Caída de Árboles sobre Fibra": "Árboles", "Falla de Energía Comercial": "Energía", "Corrosión en Equipos": "Corrosión", "Saturación en Servidor UNIFI": "Sat. UNIFI", "Falla de Inicio en UNIFI": "Inic. UNIFI", "Condiciones Climáticas": "Clima"}
        dc = df_m.groupby('causa_raiz').size().reset_index(name='Alertas').sort_values('Alertas', ascending=False)
        dc['Causa'] = dc['causa_raiz'].map(lambda x: cc.get(x, str(x).split()[0]))
        fig_p = px.pie(dc, names='Causa', values='Alertas', hole=0.4, color_discrete_sequence=PALETA_CORP)
        fig_p.update_traces(textinfo='percent+label', hoverinfo='label+value'); fig_p.update_layout(showlegend=False, margin=dict(l=0,r=0,t=0,b=0), paper_bgcolor="rgba(0,0,0,0)")
        _, c_pie, _ = st.columns([1, 2, 1])
        with c_pie: sel_p = st.plotly_chart(fig_p, use_container_width=True, on_select="rerun", selection_mode="points")
        if sel_p and sel_p.selection.point_indices:
            cx = dc.iloc[sel_p.selection.point_indices[0]]['Causa']
            st.info(f"🔍 **Detalle:** '{cx}'"); st.dataframe(df_m[df_m['causa_raiz'].map(lambda x: cc.get(x, str(x).split()[0])) == cx][['fecha_inicio', 'zona', 'equipo_afectado', 'duracion_horas', 'descripcion']], hide_index=True)

        st.divider()
        st.markdown("### 📊 Desglose de Afectaciones por Equipo y Servicio")
        de = df_m.groupby('equipo_afectado').size().reset_index(name='Fallos').sort_values('Fallos')
        fe = px.bar(de, x='Fallos', y='equipo_afectado', orientation='h', color_discrete_sequence=['#f15c22'], text_auto='.0f')
        fe.update_traces(textposition='outside'); fe.update_layout(margin=dict(l=0,r=0,t=30,b=0), paper_bgcolor="rgba(0,0,0,0)", xaxis_title="", yaxis_title="")
        st.plotly_chart(fe, use_container_width=True)

        ds = df_m.groupby('servicio').size().reset_index(name='Eventos').sort_values('Eventos')
        fs = px.bar(ds, x='Eventos', y='servicio', orientation='h', color='servicio', color_discrete_sequence=PALETA_CORP, text_auto='.0f')
        fs.update_traces(textposition='outside'); fs.update_layout(showlegend=False, margin=dict(l=0,r=0,t=30,b=0), paper_bgcolor="rgba(0,0,0,0)", xaxis_title="", yaxis_title="")
        st.plotly_chart(fs, use_container_width=True)

        st.divider()
        st.markdown("### 📈 Análisis Temporal")
        dt_trend = df_m.copy()
        dt_trend['Dia'] = pd.to_datetime(dt_trend['fecha_convertida']).dt.day
        da = dt_trend.groupby('Dia').size().reset_index(name='Eventos'); da['Mes'] = 'Actual'
        if m_idx > 1 and not df_p.empty:
            dp = df_p.copy()
            dp['Dia'] = pd.to_datetime(dp['fecha_inicio'], errors='coerce').dt.day
            dpa = dp.groupby('Dia').size().reset_index(name='Eventos'); dpa['Mes'] = 'Anterior'
            da = pd.concat([da, dpa])
        ft = px.line(da, x='Dia', y='Eventos', color='Mes', color_discrete_map={"Actual": "#f15c22", "Anterior": "rgba(255,255,255,0.3)"}, markers=True)
        ft.update_layout(margin=dict(l=0,r=0,t=30,b=0), paper_bgcolor="rgba(0,0,0,0)", xaxis_title="Día", yaxis_title="Fallas"); st.plotly_chart(ft, use_container_width=True)
            
        dg = df_m.dropna(subset=['fecha_inicio', 'hora_inicio', 'fecha_fin', 'hora_fin']).copy()
        if not dg.empty:
            dg['Start'] = pd.to_datetime(dg['fecha_inicio'].astype(str) + ' ' + dg['hora_inicio'].astype(str))
            dg['End'] = pd.to_datetime(dg['fecha_fin'].astype(str) + ' ' + dg['hora_fin'].astype(str))
            fg = px.timeline(dg, x_start="Start", x_end="End", y="zona", color="causa_raiz", color_discrete_sequence=PALETA_CORP, title="Gantt Simultáneo")
            fg.update_yaxes(autorange="reversed"); fg.update_traces(marker_line_width=1, marker_line_color="rgba(255,255,255,0.5)")
            fg.update_layout(margin=dict(l=0,r=0,t=30,b=0), paper_bgcolor="rgba(0,0,0,0)"); st.plotly_chart(fg, use_container_width=True)

# ---------------------------------------------------------------------
# [TABS DE ADMINISTRADOR]
# ---------------------------------------------------------------------
if st.session_state.role == 'admin':
    with tabs[1]:
        st.title("📝 Ingreso Operativo")
        cf, ccx = st.columns([2, 1], gap="large")
        with cf:
            with st.container(border=True):
                z = st.text_input("📍 Nodo o Zona")
                c1, c2 = st.columns(2)
                s = c1.selectbox("🌐 Servicio", ["Internet", "Cable TV", "IPTV"])
                cat = c2.selectbox("🏢 Segmento", ["Red Multinet", "Cliente Corporativo"])
                eq = st.selectbox("🖥️ Equipo", ["OLT", "RB/Mikrotik", "Switch", "ONU", "Servidor", "Fibra Principal", "Caja NAP", "Sistema UNIFI", "Antenas"])
                st.divider()
                ct1, ct2 = st.columns(2)
                with ct1:
                    fi = st.date_input("📅 Inicio")
                    hi_val = st.time_input("Hora Apertura") if st.toggle("🕒 Hora Inicio") else None
                    if not hi_val: st.info("ℹ️ Sin hora inicio, no se calcula duración.")
                with ct2:
                    ff = st.date_input("📅 Cierre")
                    hf_val = st.time_input("Hora Cierre") if st.toggle("🕒 Hora Cierre") else None
                    if not hf_val: st.info("ℹ️ Sin hora cierre, no se calcula duración.")
                dur = max(0, round((datetime.combine(ff, hf_val) - datetime.combine(fi, hi_val)).total_seconds() / 3600, 2)) if hi_val and hf_val else 0
                st.divider()
                cf1, cf2 = st.columns(2)
                with cf1:
                    cl_f = 1 if cat == "Cliente Corporativo" else st.number_input("👤 Afectados", 0)
                    if cat == "Cliente Corporativo": st.info("🏢 Segmento Corp: 1 enlace auto.", icon="ℹ️")
                cr = cf2.selectbox("🛠️ Causa Raíz", CAUSAS_RAIZ)
                desc = st.text_area("📝 Descripción")

                if st.button("💾 Guardar Registro", type="primary"):
                    if fi > ff or (fi == ff and hi_val and hf_val and hi_val > hf_val): st.toast("❌ Error lógico de fechas.", icon="🚨")
                    else:
                        with st.spinner("Guardando..."):
                            try:
                                with engine.begin() as conn:
                                    conn.execute(text("INSERT INTO incidents (zona, servicio, categoria, equipo_afectado, fecha_inicio, hora_inicio, fecha_fin, hora_fin, clientes_afectados, causa_raiz, descripcion, duracion_horas, conocimiento_tiempos) VALUES (:z, :s, :c, :e, :fi, :hi, :ff, :hf, :cl, :cr, :d, :dur, :con)"), {"z": z, "s": s, "c": cat, "e": eq, "fi": fi.strftime("%Y-%m-%d"), "hi": hi_val.strftime("%H:%M:%S") if hi_val else None, "ff": ff.strftime("%Y-%m-%d"), "hf": hf_val.strftime("%H:%M:%S") if hf_val else None, "cl": cl_f, "cr": cr, "d": desc, "dur": dur, "con": "Total" if hi_val and hf_val else "Parcial"})
                                log_audit("INSERT", f"Falla en {z}")
                                load_data_mes.clear(fi.month, fi.year)
                                st.toast("✅ Guardado Exitosamente.", icon="🎉"); time.sleep(1); st.rerun()
                            except Exception as e: st.toast(f"Error: {e}")
        with ccx:
            st.markdown("#### 🕒 Recientes")
            if not df_m.empty:
                for _, r in df_m.tail(5).sort_values('id', ascending=False).iterrows():
                    with st.container(border=True): st.markdown(f"**📍 {r['zona']}**"); st.caption(f"🔧 {r['causa_raiz'][:20]}... | ⏳ {r['duracion_horas']}h")

    with tabs[2]:
        st.markdown("### 🗂️ Auditoría de Base de Datos")
        if df_m.empty: st.info("No hay datos.")
        else:
            c_s, c_p = st.columns([4, 1])
            bq = c_s.text_input("🔎 Buscar:", placeholder="Filtrar...")
            df_d = df_m[df_m.astype(str).apply(lambda x: x.str.contains(bq, case=False, na=False)).any(axis=1)].copy() if bq else df_m.copy()
            
            tot_p = max(1, math.ceil(len(df_d) / 15))
            pg = c_p.number_input("Página", 1, tot_p, 1, key="p_bd")
            
            df_p = df_d.iloc[(pg-1)*15 : pg*15].copy()
            df_p.insert(0, "Sel", False)
            
            ed_df = st.data_editor(df_p.drop(columns=[c for c in ['fecha_convertida','mes_nombre','lat','lon'] if c in df_p], errors='ignore'), column_config={"Sel": st.column_config.CheckboxColumn("✔", default=False), "id": None, "worksheet_name": None, "gsheet_id": None}, use_container_width=True, hide_index=True)
            
            f_del = ed_df[ed_df["Sel"] == True]
            h_cam = not df_p.drop(columns=[c for c in ['fecha_convertida','mes_nombre','lat','lon','Sel'] if c in df_p], errors='ignore').reset_index(drop=True).equals(ed_df.drop(columns=['Sel']).reset_index(drop=True))
            
            if not f_del.empty or h_cam:
                cb1, cb2 = st.columns(2)
                if not f_del.empty and cb1.button("🗑️ Eliminar Seleccionados", type="primary", use_container_width=True):
                    with engine.begin() as cn:
                        for rid in f_del['id']: cn.execute(text("DELETE FROM incidents WHERE id=:id"), {"id": int(rid)})
                    log_audit("DELETE", f"Eliminados {len(f_del)} regs."); load_data_mes.clear(m_idx, a_sel); st.toast("Eliminados", icon="✅"); time.sleep(1); st.rerun()
                if h_cam and cb2.button("💾 Guardar Cambios", type="primary", use_container_width=True):
                    with engine.begin() as cn:
                        for i, r in ed_df.iterrows():
                            o = df_p.drop(columns=[c for c in ['fecha_convertida','mes_nombre','lat','lon','Sel'] if c in df_p], errors='ignore').iloc[i]
                            if not o.equals(r): cn.execute(text("UPDATE incidents SET zona=:z, servicio=:s, categoria=:c, equipo_afectado=:e, fecha_inicio=:fi, hora_inicio=:hi, fecha_fin=:ff, hora_fin=:hf, clientes_afectados=:cl, causa_raiz=:cr, descripcion=:d, duracion_horas=:dur WHERE id=:id"), {"z": r.get('zona',''), "s": r.get('servicio',''), "c": r.get('categoria',''), "e": r.get('equipo_afectado',''), "fi": r.get('fecha_inicio',''), "hi": None if pd.isna(r.get('hora_inicio')) else r.get('hora_inicio'), "ff": r.get('fecha_fin',''), "hf": None if pd.isna(r.get('hora_fin')) else r.get('hora_fin'), "cl": int(r.get('clientes_afectados',0)), "cr": r.get('causa_raiz',''), "d": r.get('descripcion',''), "dur": float(r.get('duracion_horas',0)), "id": int(r['id'])})
                    log_audit("UPDATE", "Registros editados."); load_data_mes.clear(m_idx, a_sel); st.toast("Guardado", icon="✅"); time.sleep(1); st.rerun()
            st.divider()
            c_e1, c_e2 = st.columns(2)
            with c_e1: st.download_button("📥 Descargar archivo CSV", df_d.to_csv(index=False).encode(), f"NOC_{m_sel}.csv", "text/csv", use_container_width=True)
            with c_e2:
                dt, acds, slas, mttrs, cls, mhs = calc_kpis(df_m, d_mes * 24)
                st.download_button("📥 Descargar archivo PDF", generar_pdf(m_sel, a_sel, mttrs, slas, acds, cls, dt/24.0, df_m), f"NOC_{m_sel}.pdf", "application/pdf", use_container_width=True)

    with tabs[3]:
        cu, clg = st.columns([1, 2], gap="large")
        with cu:
            st.markdown("#### 👤 Crear Usuario")
            with st.form("form_u", clear_on_submit=True):
                nu, np, nrl, npr, nrs = st.text_input("Usuario"), st.text_input("Clave", type="password"), st.selectbox("Rol", ["viewer", "admin"]), st.text_input("Pregunta Seg."), st.text_input("Respuesta")
                if st.form_submit_button("Crear") and nu and np:
                    try:
                        with engine.begin() as cn: cn.execute(text("INSERT INTO users (username, password_hash, role, pregunta, respuesta) VALUES (:u,:h,:r,:p,:rs)"), {"u":nu,"h":hash_pw(np),"r":nrl,"p":npr,"rs":nrs})
                        st.toast(f"Creado {nu}", icon="✅"); time.sleep(1); st.rerun()
                    except: st.toast("Error: Usuario duplicado", icon="❌")
        with clg:
            st.markdown("#### 📋 Panel de Usuarios Activos")
            try:
                with engine.connect() as cn: df_usrs = pd.read_sql(text("SELECT id, username, role, is_banned, failed_attempts FROM users"), cn)
                df_usrs.insert(0, "Seleccionar", False)
                ed_usrs = st.data_editor(df_usrs, column_config={"Seleccionar": st.column_config.CheckboxColumn("✔", default=False), "id": None, "username": "Usuario", "role": "Rol", "is_banned": "Baneado", "failed_attempts": "Intentos Fallidos"}, use_container_width=True, hide_index=True)
                
                filas_usr_del = ed_usrs[ed_usrs["Seleccionar"] == True]
                hay_cambios_usr = not df_usrs.drop(columns=['Seleccionar']).reset_index(drop=True).equals(ed_usrs.drop(columns=['Seleccionar']).reset_index(drop=True))
                
                if not filas_usr_del.empty or hay_cambios_usr:
                    cu1, cu2 = st.columns(2)
                    if not filas_usr_del.empty and cu1.button("🗑️ Eliminar Usuarios", type="primary", use_container_width=True):
                        if "Admin" in filas_usr_del['username'].values: st.error("❌ No se puede eliminar a Admin.")
                        else:
                            with engine.begin() as conn:
                                for rid in filas_usr_del['id']: conn.execute(text("DELETE FROM users WHERE id = :id"), {"id": int(rid)})
                            st.toast("Usuarios eliminados.", icon="✅"); time.sleep(1); st.rerun()
                    if hay_cambios_usr and cu2.button("💾 Guardar Permisos", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for i, e_row in ed_usrs.iterrows():
                                o_row = df_usrs.drop(columns=['Seleccionar']).iloc[i]
                                if not o_row.equals(e_row): conn.execute(text("UPDATE users SET role=:r, is_banned=:b, failed_attempts=:f WHERE id=:id"), {"r": str(e_row['role']), "b": bool(e_row['is_banned']), "f": int(e_row['failed_attempts']), "id": int(e_row['id'])})
                        st.toast("Permisos actualizados.", icon="✅"); time.sleep(1); st.rerun()
            except Exception as e: st.info(f"Error cargando usuarios: {e}")
            
            st.divider()
            c_lt, c_lp = st.columns([4, 1])
            c_lt.markdown("#### 📜 Registro de Actividad (Logs)")
            try:
                with engine.connect() as cn: logs = pd.read_sql(text("SELECT timestamp as Fecha, username as Usuario, action as Accion, details as Detalles FROM audit_logs ORDER BY id DESC"), cn)
                t_lp = max(1, math.ceil(len(logs)/10))
                p_log = c_lp.number_input("Página", 1, t_lp, 1, key="plg")
                st.dataframe(logs.iloc[(p_log-1)*10 : p_log*10], use_container_width=True, hide_index=True)
            except: st.info("Sin logs.")
