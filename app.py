# =============================================================================
# MULTINET NOC — v24.0
# Stack: Streamlit + Neon (PostgreSQL) + SmartOLT API
# Autor: Senior Cloud Architect / Lead Python Developer
# =============================================================================

# --- [ETIQUETA: IMPORTS] ---
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
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                Table, TableStyle, HRFlowable)
from reportlab.lib.units import cm

# =============================================================================
# --- [ETIQUETA: CONFIGURACIÓN GLOBAL] ---
# =============================================================================
st.set_page_config(page_title="Multinet NOC", layout="wide", page_icon="🌐")
SV_TZ = pytz.timezone('America/El_Salvador')

HOURS_MONTH = 720.0  # Base para cálculo de riesgo financiero (30 días × 24 h)

# --- Paleta corporativa Multinet ---
COLOR_PRIMARY   = '#f15c22'
COLOR_SECONDARY = '#1d2c59'
COLOR_TEAL      = '#29b09d'
COLOR_WARN      = '#ff9f43'
COLOR_DANGER    = '#ff2b2b'

# --- [ETIQUETA: CATÁLOGOS POR DEFECTO — Solo para seed inicial de BD] ---
DEFAULT_ZONAS = [
    ("El Rosario",            13.4886, -89.0256),
    ("ARG",                   13.4880, -89.3200),
    ("Tepezontes",            13.6214, -89.0125),
    ("La Libertad",           13.4883, -89.3200),
    ("El Tunco",              13.4930, -89.3830),
    ("Costa del Sol",         13.3039, -88.9450),
    ("Zacatecoluca",          13.5048, -88.8710),
    ("Zaragoza",              13.5850, -89.2890),
    ("Santiago Nonualco",     13.5186, -88.9442),
    ("Rio Mar",               13.4900, -89.3500),
    ("San Salvador (Central)",13.6929, -89.2182),
]
DEFAULT_EQUIPOS = [
    "ONT", "Repetidor Wi-Fi", "Antena Ubiquiti", "OLT", "Caja NAP",
    "RB/Mikrotik", "Switch", "Servidor", "Fibra Principal", "Sistema UNIFI",
]
DEFAULT_CAUSAS = [
    ("Corte de Fibra por Terceros",     True),
    ("Corte de Fibra (No Especificado)",False),
    ("Caída de Árboles sobre Fibra",    True),
    ("Falla de Energía Comercial",      True),
    ("Corrosión en Equipos",            False),
    ("Daños por Fauna",                 True),
    ("Falla de Hardware",               False),
    ("Falla de Configuración",          False),
    ("Falla de Redundancia",            False),
    ("Saturación de Tráfico",           False),
    ("Saturación en Servidor UNIFI",    False),
    ("Falla de Inicio en UNIFI",        False),
    ("Mantenimiento Programado",        False),
    ("Vandalismo o Hurto",              True),
    ("Condiciones Climáticas",          True),
]
DEFAULT_SERVICIOS = [
    "Internet", "Cable TV (CATV)", "IPTV (Mnet+)",
    "Internet/Cable TV", "Aplicativos internos",
]
DEFAULT_CATEGORIAS = [
    "Red Multinet", "Cliente Corporativo", "Falla Interna (No afecta clientes)",
]
CAT_INTERNA = "Falla Interna (No afecta clientes)"

MESES = [
    "Enero","Febrero","Marzo","Abril","Mayo","Junio",
    "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre",
]

# --- [ETIQUETA: CSS GLOBAL] ---
st.markdown(f"""
<style>
div.stButton > button {{
    border: none !important; outline: none !important;
    box-shadow: none !important; border-radius: 8px;
    width: 100%; font-weight: 600; transition: all 0.3s ease !important;
}}
div.stButton > button:focus  {{ border: none !important; outline: none !important; box-shadow: none !important; }}
div.stButton > button:hover  {{ transform: translateY(-2px); }}
[data-testid="stMetricValue"] {{ color: #ffffff !important; font-size: 34px !important; font-weight: 800 !important; }}
[data-testid="stMetricLabel"] {{ color: #a5a8b5 !important; font-size: 14px !important; font-weight: 500 !important; }}
[data-testid="stMetricDelta"] svg {{ width: 20px; height: 20px; }}
[data-testid="stMetricDelta"] div {{ font-size: 13px !important; font-weight: 600 !important; }}
button[data-baseweb="tab"] {{
    background-color: #1e1e2f !important; border-radius: 12px 12px 0 0 !important;
    margin-right: 8px !important; padding: 13px 26px !important;
    border: 2px solid #333 !important; border-bottom: none !important; transition: all 0.3s;
}}
button[data-baseweb="tab"]:hover                   {{ background-color: #2a2a3f !important; }}
button[data-baseweb="tab"][aria-selected="true"]   {{ background-color: {COLOR_PRIMARY} !important; border-color: {COLOR_PRIMARY} !important; }}
button[data-baseweb="tab"] p                       {{ font-size: 15px !important; font-weight: 700 !important; color: #a5a8b5 !important; margin: 0 !important; }}
button[data-baseweb="tab"][aria-selected="true"] p {{ color: #ffffff !important; }}
</style>
""", unsafe_allow_html=True)

# --- Inicializar session_state ---
for _k, _v in [('form_reset', 0), ('logged_in', False), ('role', ''),
               ('username', ''), ('log_u', ''), ('log_p', ''),
               ('log_err', ''), ('log_msg', '')]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

# =============================================================================
# --- [ETIQUETA: MOTOR DE BASE DE DATOS] ---
# =============================================================================
@st.cache_resource
def get_engine():
    """Pool de conexiones persistente a Neon (PostgreSQL)."""
    return create_engine(
        st.secrets["neon_dsn"],
        pool_pre_ping=True,
        pool_recycle=300,
    )

engine = get_engine()

# --- Helpers de contraseña ---
def hash_pw(p: str) -> str:
    return bcrypt.hashpw(p.encode(), bcrypt.gensalt()).decode()

def check_pw(p: str, h: str) -> bool:
    return bcrypt.checkpw(p.encode(), h.encode())

# --- [ETIQUETA: INICIALIZACIÓN DE TABLAS] ---
def init_db():
    """
    Crea todas las tablas si no existen y hace el seed de catálogos
    solo cuando están vacíos. Idem-potente — seguro correr múltiples veces.
    """
    ddl_users = """
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
        )"""
    ddl_audit = """
        CREATE TABLE IF NOT EXISTS audit_logs (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            username VARCHAR(50),
            action VARCHAR(50),
            details TEXT
        )"""
    ddl_cat_zonas = """
        CREATE TABLE IF NOT EXISTS cat_zonas (
            id SERIAL PRIMARY KEY,
            nombre VARCHAR(150) UNIQUE NOT NULL,
            lat FLOAT DEFAULT 13.6929,
            lon FLOAT DEFAULT -89.2182
        )"""
    ddl_cat_equipos = "CREATE TABLE IF NOT EXISTS cat_equipos (id SERIAL PRIMARY KEY, nombre VARCHAR(100) UNIQUE NOT NULL)"
    ddl_cat_causas  = "CREATE TABLE IF NOT EXISTS cat_causas  (id SERIAL PRIMARY KEY, nombre VARCHAR(150) UNIQUE NOT NULL, es_externa BOOLEAN DEFAULT FALSE)"
    ddl_cat_servs   = "CREATE TABLE IF NOT EXISTS cat_servicios (id SERIAL PRIMARY KEY, nombre VARCHAR(100) UNIQUE NOT NULL)"
    ddl_incidents   = """
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
        )"""
    ddl_history = """
        CREATE TABLE IF NOT EXISTS incidents_history (
            id SERIAL PRIMARY KEY,
            incident_id INT,
            changed_by VARCHAR(50),
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            old_data TEXT,
            new_data TEXT
        )"""
    ddl_cmdb = """
        CREATE TABLE IF NOT EXISTS inventario_nodos (
            id SERIAL PRIMARY KEY,
            zona VARCHAR(150),
            subzona VARCHAR(150),
            equipo VARCHAR(100),
            clientes INT DEFAULT 0,
            UNIQUE(zona, subzona, equipo)
        )"""

    with engine.begin() as conn:
        for ddl in [ddl_users, ddl_audit, ddl_cat_zonas, ddl_cat_equipos,
                    ddl_cat_causas, ddl_cat_servs, ddl_incidents,
                    ddl_history, ddl_cmdb]:
            conn.execute(text(ddl))

        # Columnas opcionales (migración no destructiva)
        conn.execute(text("ALTER TABLE incidents ADD COLUMN IF NOT EXISTS conocimiento_tiempos VARCHAR(50) DEFAULT 'Total'"))
        conn.execute(text("ALTER TABLE incidents ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ DEFAULT NULL"))

        # Seeds condicionales
        seeds = [
            ("cat_zonas",    [({"n": n, "lat": la, "lon": lo},
                               "INSERT INTO cat_zonas (nombre,lat,lon) VALUES (:n,:lat,:lon) ON CONFLICT DO NOTHING")
                              for n, la, lo in DEFAULT_ZONAS]),
            ("cat_equipos",  [({"n": e},
                               "INSERT INTO cat_equipos (nombre) VALUES (:n) ON CONFLICT DO NOTHING")
                              for e in DEFAULT_EQUIPOS]),
            ("cat_causas",   [({"n": n, "e": ext},
                               "INSERT INTO cat_causas (nombre,es_externa) VALUES (:n,:e) ON CONFLICT DO NOTHING")
                              for n, ext in DEFAULT_CAUSAS]),
            ("cat_servicios",[({"n": s},
                               "INSERT INTO cat_servicios (nombre) VALUES (:n) ON CONFLICT DO NOTHING")
                              for s in DEFAULT_SERVICIOS]),
        ]
        for tabla, rows in seeds:
            if conn.execute(text(f"SELECT count(*) FROM {tabla}")).scalar() == 0:
                for params, sql in rows:
                    conn.execute(text(sql), params)

        # Admin raíz
        if conn.execute(text("SELECT count(*) FROM users WHERE username='Admin'")).scalar() == 0:
            conn.execute(
                text("INSERT INTO users (username,password_hash,role,pregunta,respuesta) VALUES ('Admin',:h,'admin','¿Color favorito?','azul')"),
                {"h": hash_pw("Areakde5")},
            )

try:
    init_db()
except Exception as _e:
    st.error(f"Error DB Inicialización: {_e}")

# =============================================================================
# --- [ETIQUETA: HELPERS DE CATÁLOGO — Lectura dinámica desde BD] ---
# =============================================================================
def get_zonas() -> list[tuple]:
    """Retorna lista de (nombre, lat, lon) desde cat_zonas."""
    try:
        with engine.connect() as c:
            return [(r[0], r[1], r[2]) for r in
                    c.execute(text("SELECT nombre, lat, lon FROM cat_zonas ORDER BY nombre")).fetchall()]
    except:
        return list(DEFAULT_ZONAS)

def get_cat(tabla: str) -> list[str]:
    """Retorna lista de nombres de un catálogo genérico."""
    try:
        with engine.connect() as c:
            return [r[0] for r in
                    c.execute(text(f"SELECT nombre FROM {tabla} ORDER BY nombre")).fetchall()]
    except:
        return []

def get_causas_con_flag() -> dict:
    """Retorna {nombre_causa: es_externa(bool)} desde cat_causas."""
    try:
        with engine.connect() as c:
            return {r[0]: r[1] for r in
                    c.execute(text("SELECT nombre, es_externa FROM cat_causas ORDER BY nombre")).fetchall()}
    except:
        return {}

# =============================================================================
# --- [ETIQUETA: CONEXIÓN SMARTOLT — Integración API Automática] ---
# Documentación SmartOLT: https://smartolt.com/api/
# El cache TTL de 3600s evita bloquear la UI; los datos se renuevan
# automáticamente cada hora de forma transparente al usuario.
# =============================================================================

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_smartolt_clients() -> dict:
    """
    Consulta la API de SmartOLT y devuelve un diccionario
    {nombre_olt: total_clientes_activos}.

    La función está cacheada 1 hora (ttl=3600). Si la API no responde,
    retorna un dict vacío para no bloquear el resto de la app.

    Requiere en st.secrets:
        smartolt_api_url  — ej. "https://tu-instancia.smartolt.com/api"
        smartolt_api_key  — token Bearer de la cuenta de servicio
    """
    try:
        base_url = st.secrets["smartolt_api_url"].rstrip("/")
        headers  = {
            "Authorization": f"Bearer {st.secrets['smartolt_api_key']}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
        }

        # 1. Obtener listado de OLTs
        r_olts = requests.get(f"{base_url}/olt", headers=headers, timeout=10)
        r_olts.raise_for_status()
        olts = r_olts.json()  # lista de objetos OLT

        resultado = {}
        for olt in olts:
            olt_id   = olt.get("id") or olt.get("_id")
            olt_name = (olt.get("name") or olt.get("hostname") or str(olt_id)).strip()

            # 2. Para cada OLT, pedir el conteo de ONUs activas (provisionadas)
            #    Endpoint estándar SmartOLT: GET /olt/{id}/onus?status=active
            try:
                r_onus = requests.get(
                    f"{base_url}/olt/{olt_id}/onus",
                    headers=headers,
                    params={"status": "active"},
                    timeout=10,
                )
                r_onus.raise_for_status()
                data_onus = r_onus.json()

                # SmartOLT puede devolver {"total": N, "data": [...]} o una lista directa
                if isinstance(data_onus, dict):
                    total = int(data_onus.get("total", len(data_onus.get("data", []))))
                elif isinstance(data_onus, list):
                    total = len(data_onus)
                else:
                    total = 0

                resultado[olt_name] = total
            except Exception:
                # Si falla una OLT individual, continuamos con las demás
                resultado[olt_name] = 0

        return resultado

    except Exception as ex:
        # Falló la conexión global — no bloqueamos la app
        # El error se mostrará sutilmente en la UI de registro
        return {"_error": str(ex)}


def get_clientes_smartolt(zona: str, equipo: str) -> int | None:
    """
    Intenta cruzar (zona + equipo) con el nombre de una OLT en SmartOLT.
    Retorna el total de clientes activos o None si no hay coincidencia.

    La lógica de matching es flexible: busca si el nombre de la OLT
    aparece en la combinación "zona / equipo" o viceversa.
    """
    if equipo.upper() != "OLT":
        # SmartOLT solo gestiona OLTs; para otros equipos usar CMDB local
        return None

    data = fetch_smartolt_clients()
    if "_error" in data:
        return None

    # Match flexible: busca la zona dentro del nombre registrado en SmartOLT
    zona_lower = zona.lower()
    for olt_name, total in data.items():
        if zona_lower in olt_name.lower() or olt_name.lower() in zona_lower:
            return total

    return None


def smartolt_status_badge() -> str:
    """Retorna un emoji de estado para la barra lateral."""
    data = fetch_smartolt_clients()
    if "_error" in data:
        return "🔴 SmartOLT: Sin conexión"
    if not data:
        return "🟡 SmartOLT: Sin OLTs"
    total_olts = len(data)
    total_cls  = sum(v for v in data.values())
    return f"🟢 SmartOLT: {total_olts} OLTs · {total_cls:,} clientes activos"


# =============================================================================
# --- [ETIQUETA: CARGA DE DATOS — Consulta principal con cache de 60s] ---
# =============================================================================
@st.cache_data(ttl=60, show_spinner=False)
def load_data_rango(
    fecha_ini: date, fecha_fin: date,
    include_deleted: bool = False,
    zona_filtro: str = "Todas",
    serv_filtro: str = "Todos",
    seg_filtro:  str = "Todos",
) -> pd.DataFrame:
    """
    Carga incidentes de Neon para el rango de fechas indicado.
    Usa parámetros posicionales para que el cache de Streamlit
    funcione correctamente (los kwargs son serializables).
    """
    s_date = datetime.combine(fecha_ini, datetime_time(0, 0, 0))
    e_date = datetime.combine(fecha_fin, datetime_time(23, 59, 59))

    conditions = [
        "((inicio_incidente >= :s AND inicio_incidente <= :e)"
        " OR (fin_incidente   >= :s AND fin_incidente   <= :e)"
        " OR (inicio_incidente <= :s AND fin_incidente  >= :e))",
    ]
    if not include_deleted:
        conditions.append("deleted_at IS NULL")
    else:
        conditions.append("deleted_at IS NOT NULL")

    # Filtros dinámicos — valores controlados (no user-input directo)
    if zona_filtro != "Todas":
        conditions.append(f"zona = '{zona_filtro.replace(chr(39), chr(39)*2)}'")
    if serv_filtro != "Todos":
        conditions.append(f"servicio = '{serv_filtro.replace(chr(39), chr(39)*2)}'")
    if seg_filtro  != "Todos":
        conditions.append(f"categoria = '{seg_filtro.replace(chr(39), chr(39)*2)}'")

    q = "SELECT * FROM incidents WHERE " + " AND ".join(conditions) + " ORDER BY inicio_incidente ASC"

    try:
        with engine.connect() as conn:
            return pd.read_sql(text(q), conn, params={"s": s_date, "e": e_date})
    except:
        return pd.DataFrame()


# =============================================================================
# --- [ETIQUETA: ENRIQUECIMIENTO DE DATOS] ---
# Añade columnas calculadas (Severidad, es_externa, zona_completa)
# sin modificar la BD — puramente en memoria.
# =============================================================================
def enriquecer(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    # Normalización timezone-aware → America/El_Salvador
    for col in ('inicio_incidente', 'fin_incidente'):
        df[col] = pd.to_datetime(df[col], utc=True).dt.tz_convert(SV_TZ)

    df['duracion_horas']     = pd.to_numeric(df['duracion_horas'],     errors='coerce').fillna(0.0)
    df['clientes_afectados'] = pd.to_numeric(df['clientes_afectados'], errors='coerce').fillna(0).astype(int)

    causas_ext_map = get_causas_con_flag()

    def _severidad(r) -> str:
        if r['categoria'] == CAT_INTERNA:                                      return '🟢 P4 (Interna)'
        if r['duracion_horas'] >= 12 or r['clientes_afectados'] >= 1000:       return '🔴 P1 (Crítica)'
        if r['duracion_horas'] >= 4  or r['clientes_afectados'] >= 300:        return '🟠 P2 (Alta)'
        return '🟡 P3 (Media)'

    df['Severidad']     = df.apply(_severidad, axis=1)
    df['es_externa']    = df['causa_raiz'].map(lambda x: causas_ext_map.get(x, False))
    df['zona_completa'] = df.apply(
        lambda r: f"{r['zona']} (General)"
                  if r.get('afectacion_general', True)
                  else f"{r['zona']} - {r['subzona']}", axis=1)
    return df


# =============================================================================
# --- [ETIQUETA: MOTOR DE KPIs] ---
# Lógica matemática segmentada en 4 tiers + métricas globales.
# NO modificar sin revisar la documentación de metodología NOC.
# =============================================================================
def _merge_intervals(intervals: list) -> list:
    """
    Fusiona intervalos de tiempo solapados para calcular downtime real
    sin contar doble cuando múltiples nodos caen simultáneamente.
    Complejidad: O(n log n).
    """
    if not intervals:
        return []
    srt    = sorted(intervals, key=lambda x: x[0])
    merged = [list(srt[0])]
    for s, e in srt[1:]:
        if s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])
    return merged


def calc_kpis(df: pd.DataFrame, fecha_ini: date, fecha_fin: date) -> dict:
    """
    Calcula todos los KPIs segmentados por tier:
      - global: SLA, MTBF, acumulado, pico, p1
      - t1    : Fallas completas (tiempos + clientes > 0) → MTTR, ACD
      - t2    : Fallas red pura  (tiempos exactos, 0 clientes)
      - t3    : Fallas incompletas (sin hora inicio/fin)
      - int   : Fallas internas / mantenimiento
    """
    h_tot = ((datetime.combine(fecha_fin, datetime_time(23, 59, 59))
              - datetime.combine(fecha_ini, datetime_time(0, 0, 0))
              ).total_seconds()) / 3600.0

    rng_s = SV_TZ.localize(datetime.combine(fecha_ini, datetime_time(0, 0, 0)))
    rng_e = SV_TZ.localize(datetime.combine(fecha_fin, datetime_time(23, 59, 59)))

    # Estructura base — valores "verde" si no hay datos
    base = {
        "global": {"sla": 100.0, "total_fallas": 0, "mtbf": h_tot,
                   "db": 0.0, "mh": 0.0, "p1": 0},
        "t1":     {"fallas": 0, "mttr": 0.0, "acd": 0.0, "clientes": 0},
        "t2":     {"fallas": 0, "mttr": 0.0},
        "t3":     {"fallas": 0, "clientes_est": 0},
        "int":    {"fallas": 0, "mttr": 0.0},
    }
    if df.empty:
        return base

    df_int = df[df['categoria'] == CAT_INTERNA]
    df_ext = df[df['categoria'] != CAT_INTERNA]

    # ── Internas ──
    base["int"]["fallas"] = len(df_int)
    if not df_int.empty:
        iv = df_int[df_int['duracion_horas'] > 0]
        base["int"]["mttr"] = float(iv['duracion_horas'].mean()) if not iv.empty else 0.0

    if df_ext.empty:
        return base

    base["global"]["total_fallas"] = len(df_ext)
    base["global"]["p1"]           = int((df_ext['Severidad'] == '🔴 P1 (Crítica)').sum())

    # ── Tier 3: Incompletas (sin hora exacta) ──
    df_t3 = df_ext[df_ext['conocimiento_tiempos'] != 'Total']
    base["t3"]["fallas"]       = len(df_t3)
    base["t3"]["clientes_est"] = int(df_t3['clientes_afectados'].sum())

    # ── Exactas ──
    df_exact = df_ext[df_ext['conocimiento_tiempos'] == 'Total']
    if not df_exact.empty:
        base["global"]["db"] = float(df_exact['duracion_horas'].sum())
        base["global"]["mh"] = float(df_exact['duracion_horas'].max())

    # ── Tier 2: Red pura (exactas, 0 clientes) ──
    df_t2 = df_exact[df_exact['clientes_afectados'] == 0]
    base["t2"]["fallas"] = len(df_t2)
    if not df_t2.empty:
        base["t2"]["mttr"] = float(df_t2['duracion_horas'].mean())

    # ── Tier 1: Impacto real (exactas, clientes > 0) ──
    df_t1 = df_exact[df_exact['clientes_afectados'] > 0]
    base["t1"]["fallas"] = len(df_t1)
    if not df_t1.empty:
        base["t1"]["mttr"]    = float(df_t1['duracion_horas'].mean())
        base["t1"]["clientes"]= int(df_t1['clientes_afectados'].sum())
        total_hc              = (df_t1['duracion_horas'] * df_t1['clientes_afectados']).sum()
        base["t1"]["acd"]     = float(total_hc / base["t1"]["clientes"]) if base["t1"]["clientes"] > 0 else 0.0

    # ── SLA con merge de intervalos (anti doble-descuento) ──
    if not df_exact.empty:
        s_cl  = df_exact['inicio_incidente'].clip(lower=rng_s)
        e_cl  = df_exact['fin_incidente'].clip(upper=rng_e)
        valid = (s_cl <= e_cl)
        ivs   = [[s, e] for s, e in zip(s_cl[valid], e_cl[valid])]
        merged = _merge_intervals(ivs)
        t_down = sum((e - s).total_seconds() for s, e in merged) / 3600.0
    else:
        t_down = 0.0

    base["global"]["sla"]  = max(0.0, min(100.0, (h_tot - t_down) / h_tot * 100)) if h_tot > 0 else 100.0
    n_exact                = len(df_exact)
    base["global"]["mtbf"] = float((h_tot - t_down) / n_exact) if n_exact > 0 else float(h_tot)

    return base


# =============================================================================
# --- [ETIQUETA: AUDITORÍA] ---
# =============================================================================
def log_audit(action: str, detail: str):
    """Inserta un registro en audit_logs. No lanza excepciones al usuario."""
    try:
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO audit_logs (timestamp,username,action,details) VALUES (:t,:u,:a,:d)"),
                {"t": datetime.now(SV_TZ).replace(tzinfo=None),
                 "u": st.session_state.get("username", "?"),
                 "a": action, "d": detail},
            )
    except:
        pass


# =============================================================================
# --- [ETIQUETA: GENERACIÓN DE PDF] ---
# Usa ReportLab. NO alterar la estructura de tablas sin prueba previa.
# =============================================================================
def generar_pdf(label_periodo: str, kpis: dict, df: pd.DataFrame) -> bytes:
    RL_ORANGE = rl_colors.HexColor(COLOR_PRIMARY)
    RL_BLUE   = rl_colors.HexColor(COLOR_SECONDARY)
    RL_TEAL   = rl_colors.HexColor(COLOR_TEAL)
    RL_LIGHT  = rl_colors.HexColor('#f5f5f5')
    RL_GRAY   = rl_colors.HexColor('#888888')
    RL_LGRAY  = rl_colors.HexColor('#dddddd')

    def mk_style(name, size, font='Helvetica', color=rl_colors.black, **kw):
        return ParagraphStyle(name, fontSize=size, fontName=font, textColor=color, **kw)

    s_title  = mk_style('t',  22, 'Helvetica-Bold', RL_BLUE,  spaceAfter=2)
    s_sub    = mk_style('s',  11, 'Helvetica',       RL_GRAY,  spaceAfter=14)
    s_period = mk_style('pe', 11, 'Helvetica-Bold',  RL_ORANGE,spaceAfter=2)
    s_body   = mk_style('b',   9, 'Helvetica',       rl_colors.black, spaceAfter=4)
    s_sec    = mk_style('h',  13, 'Helvetica-Bold',  RL_BLUE,  spaceBefore=14, spaceAfter=6)
    s_foot   = mk_style('f',   7, 'Helvetica',       RL_GRAY)

    _base_ts = [
        ('BACKGROUND',    (0,0),  (-1,0),   None),        # placeholder, set per table
        ('TEXTCOLOR',     (0,0),  (-1,0),   rl_colors.white),
        ('FONTNAME',      (0,0),  (-1,0),   'Helvetica-Bold'),
        ('FONTSIZE',      (0,0),  (-1,0),   9),
        ('ROWBACKGROUNDS',(0,1),  (-1,-1),  [RL_LIGHT, rl_colors.white]),
        ('FONTNAME',      (0,1),  (-1,-1),  'Helvetica'),
        ('FONTSIZE',      (0,1),  (-1,-1),  9),
        ('VALIGN',        (0,0),  (-1,-1),  'MIDDLE'),
        ('GRID',          (0,0),  (-1,-1),  0.4, RL_LGRAY),
        ('ROWHEIGHT',     (0,0),  (-1,-1),  20),
        ('LEFTPADDING',   (0,0),  (-1,-1),  7),
        ('RIGHTPADDING',  (0,0),  (-1,-1),  7),
        ('TEXTCOLOR',     (1,1),  (1,-1),   RL_ORANGE),
        ('FONTNAME',      (1,1),  (1,-1),   'Helvetica-Bold'),
        ('ALIGN',         (1,0),  (1,-1),   'CENTER'),
    ]

    def tbl_style(hdr_color):
        ts = list(_base_ts)
        ts[0] = ('BACKGROUND', (0,0), (-1,0), hdr_color)
        return TableStyle(ts)

    buffer  = BytesIO()
    doc     = SimpleDocTemplate(buffer, pagesize=A4,
                                leftMargin=2*cm, rightMargin=2*cm,
                                topMargin=2*cm, bottomMargin=2*cm)
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

    # Sección 1: KPIs globales
    story.append(Paragraph("1. Métricas Operativas Principales", s_sec))
    kpi_rows = [
        ['Indicador',                      'Valor',                                       'Descripción'],
        ['SLA Global (Disponibilidad)',     f"{kpis['global']['sla']:.3f}%",               'Intervalos fusionados; sin doble descuento'],
        ['MTTR Real',                       f"{kpis['t1']['mttr']:.2f} horas",             'Resolución promedio (clientes > 0)'],
        ['ACD Real',                        f"{kpis['t1']['acd']:.2f} horas",              'Afectación promedio percibida por el usuario'],
        ['Impacto Acumulado',               f"{kpis['global']['db']/24:.2f} días",         'Total horas caídas / 24'],
        ['Clientes Impactados',             f"{kpis['t1']['clientes']:,}",                 'Suma total de usuarios con corte confirmado'],
        ['MTBF (Estabilidad)',              f"{kpis['global']['mtbf']:.1f} horas",         'Tiempo medio entre fallas'],
        ['Fallas P1 Críticas',             f"{kpis['global']['p1']}",                     '>12 h duración o >1 000 clientes'],
    ]
    t = Table(kpi_rows, colWidths=[5.5*cm, 3*cm, 8.5*cm])
    t.setStyle(tbl_style(RL_BLUE))
    story += [t, Spacer(1, 0.4*cm)]

    # Sección 2: Salud de datos
    story.append(Paragraph("2. Salud de Datos / Pendientes", s_sec))
    pend_rows = [
        ['Estado',                             'Cantidad'],
        ['Fallas sin tiempo exacto',           str(kpis['t3']['fallas'])],
        ['Fallas sin clientes definidos',      str(kpis['t2']['fallas'])],
        ['Incidentes internos',                str(kpis['int']['fallas'])],
    ]
    tp = Table(pend_rows, colWidths=[10*cm, 4*cm])
    tp.setStyle(tbl_style(RL_TEAL))
    story += [tp, Spacer(1, 0.4*cm)]

    # Sección 3: Zonas top
    if not df.empty:
        df_ext = df[df['categoria'] != CAT_INTERNA]
        if not df_ext.empty:
            story.append(Paragraph("3. Zonas con Mayor Afectación", s_sec))
            top_z = df_ext.groupby('zona_completa')['duracion_horas'].sum().nlargest(8).reset_index()
            z_rows = [['Zona / Nodo', 'Horas', 'Días Equiv.']]
            for _, r in top_z.iterrows():
                z_rows.append([str(r['zona_completa']),
                               f"{r['duracion_horas']:.1f} h",
                               f"{r['duracion_horas']/24:.2f}"])
            tz = Table(z_rows, colWidths=[9*cm, 4*cm, 4*cm])
            tz.setStyle(tbl_style(RL_TEAL))
            story += [tz, Spacer(1, 0.4*cm)]

    story += [
        Spacer(1, 1*cm),
        HRFlowable(width="100%", thickness=0.5, color=RL_LGRAY, spaceAfter=6),
        Paragraph(f"MULTINET NOC  ·  {now_str}  ·  Documento Confidencial", s_foot),
    ]
    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()


# =============================================================================
# --- [ETIQUETA: SISTEMA DE AUTENTICACIÓN] ---
# =============================================================================
def do_login():
    u, p = st.session_state.log_u, st.session_state.log_p
    try:
        with engine.begin() as conn:
            ud = conn.execute(
                text("SELECT id,password_hash,role,failed_attempts,locked_until,is_banned FROM users WHERE username=:u"),
                {"u": u},
            ).fetchone()
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
    except Exception as ex:
        st.session_state.log_err = f"Error de conexión: {ex}"
    st.session_state.log_u = ""
    st.session_state.log_p = ""


# --- Pantalla de Login ---
if not st.session_state.logged_in:
    st.markdown("<div style='margin-top:10vh;'></div>", unsafe_allow_html=True)
    _, col_c, _ = st.columns([1, 1.2, 1])
    with col_c:
        if st.session_state.log_msg:
            st.toast(st.session_state.log_msg, icon="✅")
            st.session_state.log_msg = ""
        if st.session_state.log_err:
            st.error(st.session_state.log_err)
            st.session_state.log_err = ""
        with st.container(border=True):
            st.markdown("""
            <div style='text-align:center;padding:20px 0 10px 0;'>
            <div style='font-size:46px;'>🔐</div>
            <h2 style='margin:10px 0 4px;color:#fff;font-weight:700;'>Acceso NOC Central</h2>
            </div>""", unsafe_allow_html=True)
            st.text_input("Usuario",    key="log_u")
            st.text_input("Contraseña", key="log_p", type="password")
            st.write("")
            _, btn_col, _ = st.columns([1, 2, 1])
            with btn_col:
                st.button("Iniciar Sesión", type="primary", on_click=do_login, use_container_width=True)
            st.write("")
            with st.expander("¿Olvidó su contraseña?"):
                ru = st.text_input("Ingrese su usuario:", key="pw_reset_user")
                if ru:
                    try:
                        with engine.connect() as conn:
                            ud2 = conn.execute(
                                text("SELECT pregunta,respuesta FROM users WHERE username=:u"),
                                {"u": ru},
                            ).fetchone()
                        if ud2:
                            st.info(f"**Pregunta:** {ud2[0]}")
                            rr = st.text_input("Respuesta:", type="password", key="pw_reset_ans")
                            if rr:
                                if rr.strip().lower() == str(ud2[1]).lower():
                                    np_r = st.text_input("Nueva contraseña:", type="password", key="pw_reset_new")
                                    if st.button("Actualizar contraseña") and np_r:
                                        with engine.begin() as c2:
                                            c2.execute(
                                                text("UPDATE users SET password_hash=:h, failed_attempts=0, locked_until=NULL, is_banned=FALSE WHERE username=:u"),
                                                {"h": hash_pw(np_r), "u": ru},
                                            )
                                        st.session_state.log_msg = "Contraseña restablecida."
                                        st.rerun()
                                else:
                                    st.error("❌ Respuesta incorrecta.")
                        else:
                            st.error("❌ Usuario no encontrado.")
                    except:
                        pass
    st.stop()


# =============================================================================
# --- [ETIQUETA: BARRA LATERAL] ---
# =============================================================================
with st.sidebar:
    st.caption(f"👤 **{st.session_state.username}** ({st.session_state.role.capitalize()})  |  NOC v24.0")

    # Estado SmartOLT (no bloquea la UI — cacheado)
    st.caption(smartolt_status_badge())

    st.divider()

    # Selector de periodo mensual
    anio_act = datetime.now(SV_TZ).year
    anios    = sorted({anio_act+1, anio_act, anio_act-1, anio_act-2}, reverse=True)
    a_sel    = st.selectbox("🗓️ Año",  anios, index=anios.index(anio_act))
    m_sel    = st.selectbox("📅 Mes",  MESES, index=datetime.now(SV_TZ).month - 1)
    m_idx    = MESES.index(m_sel) + 1

    last_day  = calendar.monthrange(a_sel, m_idx)[1]
    fecha_ini = date(a_sel, m_idx, 1)
    fecha_fin = date(a_sel, m_idx, last_day)
    label_periodo = f"{m_sel} {a_sel}"

    st.divider()

    # Filtros de contexto
    zonas_lista = [z[0] for z in get_zonas()]
    z_sel   = st.selectbox("🗺️ Zona",     ["Todas"]  + zonas_lista)
    srv_sel = st.selectbox("🌐 Servicio", ["Todos"]  + get_cat("cat_servicios"))
    seg_sel = st.selectbox("🏢 Segmento", ["Todos"]  + DEFAULT_CATEGORIAS)

    # Carga principal del mes seleccionado
    df_m = enriquecer(load_data_rango(
        fecha_ini, fecha_fin, False, z_sel, srv_sel, seg_sel))

    # Mes anterior (para deltas comparativos)
    p_m_idx   = 12 if m_idx == 1 else m_idx - 1
    p_a_sel   = a_sel - 1 if m_idx == 1 else a_sel
    p_last    = calendar.monthrange(p_a_sel, p_m_idx)[1]
    p_fi      = date(p_a_sel, p_m_idx, 1)
    p_ff      = date(p_a_sel, p_m_idx, p_last)
    df_prev   = enriquecer(load_data_rango(p_fi, p_ff, False, z_sel, srv_sel, seg_sel))

    st.divider()

    # Descarga de PDF
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


# =============================================================================
# --- [ETIQUETA: DEFINICIÓN DE PESTAÑAS POR ROL] ---
# admin   → Dashboard + Registro + Historial + Configuración
# auditor → Dashboard + Registro + Historial
# viewer  → Dashboard
# =============================================================================
role = st.session_state.role

if   role == 'admin':   tab_labels = ["📊 Dashboard", "📝 Registrar Evento", "🗂️ Historial y Edición", "⚙️ Configuración"]
elif role == 'auditor': tab_labels = ["📊 Dashboard", "📝 Registrar Evento", "🗂️ Historial y Edición"]
else:                   tab_labels = ["📊 Dashboard"]

tabs = st.tabs(tab_labels)


# =============================================================================
# --- [ETIQUETA: TAB 0 — DASHBOARD] ---
# =============================================================================
with tabs[0]:
    st.title(f"📊 Rendimiento de Red: {label_periodo}")

    if df_m.empty:
        st.success("🟢 Sin fallas registradas en los filtros seleccionados.")
        st.stop()

    kpis      = calc_kpis(df_m, fecha_ini, fecha_fin)
    kpis_prev = calc_kpis(df_prev, p_fi, p_ff) if not df_prev.empty else None

    # Helper DRY para calcular deltas vs mes anterior
    def _delta(cat, key, divisor=1, fmt="{:+.2f}", suffix=""):
        if not kpis_prev:
            return None
        diff = (kpis[cat][key] - kpis_prev[cat][key]) / divisor
        return f"{fmt.format(diff)}{' '+suffix if suffix else ''}"

    # ── 1. Estado General ──
    st.markdown("### 📈 Estado General de la Red")
    st.caption("*Métricas globales calculadas con registros que poseen duración exacta.*")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("SLA Global",           f"{kpis['global']['sla']:.3f}%",
              delta=_delta('global','sla',    fmt="{:+.3f}", suffix="%"),   delta_color="normal",
              help="Disponibilidad total. Intervalos solapados fusionados — sin doble descuento.")
    c2.metric("Total Eventos",        kpis['global']['total_fallas'],
              delta=_delta('global','total_fallas', fmt="{:+.0f}"),          delta_color="inverse",
              help="Suma de todas las fallas externas registradas.")
    c3.metric("MTBF (Estabilidad)",   f"{kpis['global']['mtbf']:.1f} h",
              delta=_delta('global','mtbf',   fmt="{:+.1f}", suffix="h"),   delta_color="normal",
              help="Tiempo Medio Entre Fallas — con datos exactos.")
    c4.metric("Impacto Acumulado",    f"{kpis['global']['db']/24:.2f} días",
              delta=_delta('global','db', divisor=24, fmt="{:+.2f}", suffix="d"), delta_color="inverse",
              help="Total de horas de caída / 24.")
    c5.metric("Falla Mayor (Pico)",   f"{kpis['global']['mh']:.1f} h",
              delta=_delta('global','mh',     fmt="{:+.1f}", suffix="h"),   delta_color="inverse",
              help="Duración del evento más largo del periodo.")

    st.divider()

    # ── 2. Impacto a Clientes (Tier 1) ──
    st.markdown("### 👥 Impacto Directo a Clientes (Datos Exactos)")
    st.caption("*Solo eventos con duración exacta y clientes afectados > 0.*")

    if kpis['t1']['fallas'] == 0:
        st.info("ℹ️ Sin incidentes masivos con tiempos exactos en este periodo.")
    else:
        t1a, t1b, t1c, t1d = st.columns(4)
        t1a.metric("Fallas Completas",   kpis['t1']['fallas'],
                   delta=_delta('t1','fallas',   fmt="{:+.0f}"), delta_color="inverse",
                   help="Duración exacta + clientes > 0.")
        t1b.metric("MTTR Real",          f"{kpis['t1']['mttr']:.2f} h",
                   delta=_delta('t1','mttr',     fmt="{:+.2f}", suffix="h"), delta_color="inverse",
                   help="Tiempo promedio de resolución de fallas masivas.")
        t1c.metric("ACD Real",           f"{kpis['t1']['acd']:.2f} h",
                   delta=_delta('t1','acd',      fmt="{:+.2f}", suffix="h"), delta_color="inverse",
                   help="Afectación promedio percibida por cliente.")
        t1d.metric("Clientes Afectados", f"{kpis['t1']['clientes']:,}",
                   delta=_delta('t1','clientes', fmt="{:+.0f}"), delta_color="inverse",
                   help="Suma total de usuarios con corte confirmado.")

    st.divider()

    # ── 3. Salud del Dato ──
    st.markdown("### ⚠️ Salud de Datos y Eventos Aislados")
    col_t2, col_t3, col_int = st.columns(3)

    for container, label, emoji_on, emoji_off, metric_a, val_a, help_a, metric_b, val_b, help_b in [
        (col_t2,  "Fallas de Red Pura",       "🟡","🟢",
         "Eventos sin Clientes",  kpis['t2']['fallas'],     "Hora exacta, 0 clientes. Sí afectan SLA.",
         "MTTR Infraestructura", f"{kpis['t2']['mttr']:.2f} h", "Resolución de fallas aisladas."),
        (col_t3,  "Eventos Incompletos",       "🔴","🟢",
         "Fallas Sin Tiempo",    kpis['t3']['fallas'],      "Sin hora de inicio o cierre. Excluidas del SLA.",
         "Clientes Estimados",  f"{kpis['t3']['clientes_est']:,}", "Posibles afectados sin duración medible."),
        (col_int, "Internas / Mantenimiento",  "🔵","🟢",
         "Eventos Internos",     kpis['int']['fallas'],     "No impactan SLA externo.",
         "MTTR Interno",        f"{kpis['int']['mttr']:.2f} h",   "Tiempo de resolución interno."),
    ]:
        v = val_a if isinstance(val_a, int) else kpis['t3']['fallas']
        _ic = emoji_on if v > 0 else emoji_off
        with container:
            with st.container(border=True):
                st.markdown(f"#### {_ic} {label}")
                st.metric(metric_a, val_a, help=help_a)
                st.metric(metric_b, val_b, help=help_b)

    st.divider()

    # ── 4. Gráficos — Geográfico ──
    st.markdown("### 🗺️ Análisis Geográfico")
    zonas_coords = {z[0]: (z[1], z[2]) for z in get_zonas()}
    df_map       = df_m.copy()
    df_map['lat'] = df_map['zona'].map(lambda x: zonas_coords.get(x, (13.6929, -89.2182))[0])
    df_map['lon'] = df_map['zona'].map(lambda x: zonas_coords.get(x, (13.6929, -89.2182))[1])
    agg = (df_map.groupby(['zona_completa','lat','lon'])
           .agg(Horas=('duracion_horas','sum'), Clientes=('clientes_afectados','sum'))
           .reset_index())
    agg['Clientes_sz'] = agg['Clientes'].clip(lower=1)
    fig_map = px.scatter_mapbox(
        agg, lat="lat", lon="lon", hover_name="zona_completa",
        size="Clientes_sz", color="Horas",
        color_continuous_scale="Inferno", zoom=8.5,
        mapbox_style="carto-darkmatter",
        labels={"Clientes_sz": "Clientes"},
        title="Impacto Geográfico por Nodo",
    )
    fig_map.update_layout(margin=dict(l=0,r=0,t=40,b=0), height=480)
    st.plotly_chart(fig_map, use_container_width=True)

    # ── 5. Heatmap horario ──
    st.write("")
    dias_map   = {0:'Lunes',1:'Martes',2:'Miércoles',3:'Jueves',4:'Viernes',5:'Sábado',6:'Domingo'}
    dias_orden = list(dias_map.values())
    df_heat    = df_m.copy()
    df_heat['Día']  = pd.Categorical(df_heat['inicio_incidente'].dt.dayofweek.map(dias_map),
                                     categories=dias_orden, ordered=True)
    df_heat['Hora'] = df_heat['inicio_incidente'].dt.hour
    df_hm           = df_heat.groupby(['Día','Hora']).size().reset_index(name='Fallas')
    fig_hm = px.density_heatmap(
        df_hm, x='Hora', y='Día', z='Fallas',
        color_continuous_scale='Blues', nbinsx=24,
        title="Concentración de Fallas por Día y Hora",
    )
    fig_hm.update_layout(xaxis=dict(tickmode='linear', tick0=0, dtick=1),
                         margin=dict(l=0,r=0,t=40,b=0),
                         paper_bgcolor="rgba(0,0,0,0)", height=430)
    st.plotly_chart(fig_hm, use_container_width=True)

    st.divider()

    # ── 6. Responsabilidad y Causas ──
    st.markdown("### 📊 Responsabilidad y Causas Principales")
    c_pie, c_bar = st.columns(2)

    with c_pie:
        df_m['Tipo'] = df_m['es_externa'].map(
            {True: 'Externa (Fuerza Mayor)', False: 'Interna (NOC / Infraestructura)'})
        agg_r = df_m.groupby('Tipo').size().reset_index(name='Eventos')
        fig_p = px.pie(agg_r, names='Tipo', values='Eventos', hole=0.5,
                       color_discrete_sequence=[COLOR_TEAL, COLOR_DANGER],
                       title="Tasa de Responsabilidad")
        fig_p.update_traces(textinfo='percent', textposition='inside')
        fig_p.update_layout(legend=dict(orientation="h", yanchor="top", y=-0.1, xanchor="center", x=0.5),
                            margin=dict(l=0,r=0,t=40,b=0),
                            paper_bgcolor="rgba(0,0,0,0)", height=400)
        st.plotly_chart(fig_p, use_container_width=True)

    with c_bar:
        dc = (df_m.groupby('causa_raiz').size()
              .reset_index(name='Alertas')
              .sort_values('Alertas', ascending=True).tail(10))
        fig_b = px.bar(dc, x='Alertas', y='causa_raiz', orientation='h',
                       color_discrete_sequence=[COLOR_PRIMARY],
                       text_auto='.0f', title="Top Causas Raíz")
        fig_b.update_traces(textposition='outside')
        fig_b.update_layout(margin=dict(l=0,r=0,t=40,b=0),
                            paper_bgcolor="rgba(0,0,0,0)", height=400,
                            xaxis_title="", yaxis_title="")
        st.plotly_chart(fig_b, use_container_width=True)


# =============================================================================
# --- [ETIQUETA: TAB 1 — REGISTRAR EVENTO] ---
# Disponible para roles: admin, auditor
# Optimización de rendimiento:
#   - Se usa st.form para agrupar inputs y evitar recargas por cada keystroke.
#   - Los catálogos se leen una sola vez por sesión (cacheados en funciones).
#   - El autocompletado de clientes desde SmartOLT ocurre FUERA del form
#     para no requerir submit, usando on_change en los selectores de zona/equipo.
# =============================================================================
if role in ('admin', 'auditor') and len(tabs) > 1:
    with tabs[1]:
        st.title("📝 Registrar Evento Operativo")

        fk           = st.session_state.form_reset
        zonas_form   = [z[0] for z in get_zonas()]
        equipos_form = get_cat("cat_equipos")
        causas_form  = list(get_causas_con_flag().keys())
        servs_form   = get_cat("cat_servicios")

        # CMDB local para fallback de clientes
        try:
            cmdb_df = pd.read_sql(
                "SELECT zona, subzona, equipo, clientes FROM inventario_nodos", engine)
        except:
            cmdb_df = pd.DataFrame(columns=['zona','subzona','equipo','clientes'])

        cf, ccx = st.columns([2, 1], gap="large")

        with cf:
            # --- Selectores de Zona y Equipo FUERA del form ---
            # Esto permite que el autocompletado de SmartOLT reaccione
            # sin necesidad de hacer submit, pero sin causar recarga completa.
            with st.container(border=True):
                c_z1, c_z2 = st.columns([1,1])
                with c_z1:
                    z_f = st.selectbox("📍 Nodo Principal", zonas_form, key=f"reg_zona_{fk}")
                with c_z2:
                    st.markdown("<div style='margin-top:32px;'></div>", unsafe_allow_html=True)
                    ag = st.toggle("🚨 Falla General (Afecta todo el nodo)", value=True, key=f"reg_ag_{fk}")

                sz = st.text_input(
                    "📍 Sub-zona (Ej. Colonia Escalón)",
                    value="General" if ag else "",
                    key=f"reg_sz_{fk}", disabled=ag,
                )
                sz_db = "General" if ag else sz

                st.divider()

                c1f, c2f = st.columns(2)
                srv_f = c1f.selectbox("🌐 Servicio",  servs_form,        key=f"reg_s_{fk}")
                cat_f = c2f.selectbox("🏢 Segmento",  DEFAULT_CATEGORIAS, key=f"reg_c_{fk}")
                eq_f  = st.selectbox("🖥️ Equipo",     equipos_form,      key=f"reg_e_{fk}")

                # --- [ETIQUETA: AUTOCOMPLETADO SMARTOLT] ---
                # Prioridad: 1) SmartOLT (si equipo=OLT) → 2) CMDB local → 3) 0
                smartolt_cl = get_clientes_smartolt(z_f, eq_f)
                cmdb_cl     = 0
                if smartolt_cl is None and not cmdb_df.empty:
                    m_cmdb = cmdb_df[(cmdb_df['zona']    == z_f) &
                                     (cmdb_df['subzona'] == sz_db) &
                                     (cmdb_df['equipo']  == eq_f)]
                    if not m_cmdb.empty:
                        cmdb_cl = int(m_cmdb['clientes'].iloc[0])

                d_cl   = smartolt_cl if smartolt_cl is not None else cmdb_cl
                source = "SmartOLT 🔴" if smartolt_cl is not None else ("CMDB local" if cmdb_cl > 0 else "manual")

                st.divider()

                # Clientes — reglas por segmento
                if cat_f == "Cliente Corporativo":
                    cl_f = 1
                    st.number_input("👤 Clientes Afectados", value=1, disabled=True, key=f"reg_cl_{fk}")
                    st.caption("🏢 Corporativo: fijado en 1 enlace.")
                elif cat_f == CAT_INTERNA:
                    cl_f = 0
                    st.number_input("👤 Clientes Afectados", value=0, disabled=True, key=f"reg_cl_{fk}")
                    st.caption("🔧 Interna: fijado en 0 clientes.")
                else:
                    if smartolt_cl is not None:
                        st.info(f"🔴 **SmartOLT:** {smartolt_cl:,} clientes activos en esta OLT (actualizado hace < 1h)")
                    cl_f = st.number_input(
                        f"👤 Clientes Afectados  *(fuente: {source})*",
                        min_value=0, value=d_cl, step=1,
                        key=f"reg_cl_{fk}",
                        help="Si el equipo es OLT, el valor proviene de SmartOLT automáticamente.",
                    )

                st.divider()

                # Fechas y horas — dentro de un st.form para evitar recarga
                # por cada cambio de toggle/time_input
                with st.form(f"form_fechas_{fk}", clear_on_submit=False):
                    ct1, ct2 = st.columns(2)
                    with ct1:
                        fi     = st.date_input("📅 Fecha de Inicio", key=f"reg_fi")
                        hi_on  = st.toggle("🕒 Hora de Inicio conocida", value=False, key=f"reg_hi_on")
                        hi_val = st.time_input("Hora Apertura", key=f"reg_hi_val") if hi_on else None
                        if not hi_on:
                            st.caption("ℹ️ Sin hora de inicio → marcado como Incompleto.")
                    with ct2:
                        ff     = st.date_input("📅 Fecha de Cierre", key=f"reg_ff")
                        hf_on  = st.toggle("🕒 Hora de Cierre conocida", value=False, key=f"reg_hf_on")
                        hf_val = st.time_input("Hora Cierre", key=f"reg_hf_val") if hf_on else None
                        if not hf_on:
                            st.caption("ℹ️ Sin hora de cierre → marcado como Incompleto.")

                    dur         = 0.0
                    conocimiento = "Parcial"
                    if hi_on and hf_on:
                        dt_ini = datetime.combine(fi, hi_val)
                        dt_fin = datetime.combine(ff, hf_val)
                        if dt_fin > dt_ini:
                            dur          = round((dt_fin - dt_ini).total_seconds() / 3600, 2)
                            conocimiento = "Total"

                    cr_f   = st.selectbox("🛠️ Causa Raíz", causas_form, key=f"reg_cr")
                    desc_f = st.text_area("📝 Descripción del Evento", key=f"reg_desc")

                    submitted = st.form_submit_button("💾 Guardar Registro", type="primary")

                if submitted:
                    err = False
                    if hi_on and hf_on and (fi > ff or (fi == ff and hi_val >= hf_val)):
                        st.toast("❌ Error: el cierre no puede ser anterior al inicio.", icon="🚨")
                        err = True

                    if not err:
                        with st.spinner("Guardando en Base de Datos…"):
                            try:
                                hi_db = hi_val if hi_on else datetime_time(0, 0)
                                hf_db = hf_val if hf_on else datetime_time(0, 0)
                                idi   = SV_TZ.localize(datetime.combine(fi, hi_db))
                                idf   = SV_TZ.localize(datetime.combine(ff, hf_db))

                                with engine.begin() as conn:
                                    conn.execute(text("""
                                        INSERT INTO incidents
                                        (zona, subzona, afectacion_general, servicio, categoria,
                                         equipo_afectado, inicio_incidente, fin_incidente,
                                         clientes_afectados, causa_raiz, descripcion,
                                         duracion_horas, conocimiento_tiempos)
                                        VALUES (:z,:sz,:ag,:s,:c,:e,:idi,:idf,:cl,:cr,:d,:dur,:con)
                                    """), {
                                        "z": z_f, "sz": sz_db, "ag": ag, "s": srv_f,
                                        "c": cat_f, "e": eq_f, "idi": idi, "idf": idf,
                                        "cl": cl_f, "cr": cr_f, "d": desc_f,
                                        "dur": dur, "con": conocimiento,
                                    })
                                    conn.execute(text("""
                                        INSERT INTO inventario_nodos (zona,subzona,equipo,clientes)
                                        VALUES (:z,:sz,:e,:cl)
                                        ON CONFLICT (zona,subzona,equipo)
                                        DO UPDATE SET clientes = EXCLUDED.clientes
                                        WHERE EXCLUDED.clientes > inventario_nodos.clientes
                                    """), {"z": z_f, "sz": sz_db, "e": eq_f, "cl": cl_f})

                                log_audit("INSERT", f"Falla en {z_f} | {eq_f} | {cat_f}")
                                load_data_rango.clear()
                                st.session_state.form_reset += 1
                                st.rerun()
                            except Exception as ex:
                                st.toast(f"Error al guardar: {ex}", icon="❌")

        # Panel lateral de registros recientes
        with ccx:
            st.markdown("#### 🕒 Registros Recientes")
            if not df_m.empty:
                for _, r in df_m.sort_values('id', ascending=False).head(6).iterrows():
                    with st.container(border=True):
                        ico = "🔧" if r.get('categoria') == CAT_INTERNA else "📡"
                        st.markdown(f"**{ico} {r['zona_completa']}**")
                        st.caption(f"{str(r.get('causa_raiz',''))[:30]}… | ⏳ {r['duracion_horas']}h | 👥 {r['clientes_afectados']}")
            else:
                st.info("Sin registros en este periodo.")


# =============================================================================
# --- [ETIQUETA: TAB 2 — HISTORIAL Y EDICIÓN] ---
# Disponible para roles: admin, auditor
# =============================================================================
if role in ('admin', 'auditor') and len(tabs) > 2:
    with tabs[2]:
        st.markdown("### 🗂️ Historial, Auditoría y Edición")

        papelera = st.toggle("🗑️ Explorar Papelera de Reciclaje")
        df_audit = enriquecer(load_data_rango(
            fecha_ini, fecha_fin, papelera, "Todas", "Todos", "Todos"))

        if df_audit.empty:
            st.info("No hay datos en el servidor para el periodo seleccionado.")
        else:
            c_s, c_pg = st.columns([4, 1])
            bq = c_s.text_input("🔎 Buscar:", placeholder="Causa, nodo, equipo…")
            df_d = (df_audit[df_audit.astype(str)
                    .apply(lambda x: x.str.contains(bq, case=False, na=False))
                    .any(axis=1)].copy() if bq else df_audit.copy())

            tot_p   = max(1, math.ceil(len(df_d) / 15))
            pg      = c_pg.number_input("Página", 1, tot_p, 1, key="p_bd")
            df_page = df_d.iloc[(pg-1)*15 : pg*15].copy()
            df_page.insert(0, "Sel", False)

            drop_cols = [c for c in
                         ['deleted_at','Severidad','zona_completa','es_externa']
                         if c in df_page.columns]

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
            ref_df = (df_page.drop(columns=drop_cols + ['Sel'], errors='ignore')
                      .reset_index(drop=True))

            def strip_tz(s):
                if pd.api.types.is_datetime64_any_dtype(s):
                    return s.dt.tz_convert(None) if (hasattr(s.dt,'tz') and s.dt.tz) else s
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
                                conn.execute(text("UPDATE incidents SET deleted_at=NULL WHERE id=:id"),
                                             {"id": int(rid)})
                        log_audit("RESTORE", f"{len(f_sel)} registro(s).")
                        load_data_rango.clear(); st.rerun()
                else:
                    if cb1.button("🗑️ Eliminar Seleccionados", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for rid in f_sel['id']:
                                conn.execute(
                                    text("UPDATE incidents SET deleted_at=CURRENT_TIMESTAMP WHERE id=:id"),
                                    {"id": int(rid)})
                        log_audit("DELETE (SOFT)", f"{len(f_sel)} registro(s).")
                        load_data_rango.clear(); st.rerun()

            if h_cam and cb2.button("💾 Guardar Cambios", type="primary", use_container_width=True):
                with engine.begin() as conn:
                    for i, r in ed_df.iterrows():
                        o = ref_df.iloc[i]
                        if not strip_tz(o).equals(strip_tz(r.drop('Sel'))):
                            try:
                                ini_dt = pd.to_datetime(r.get('inicio_incidente'))
                                fin_dt = pd.to_datetime(r.get('fin_incidente'))
                                dur_u  = (max(0, round((fin_dt - ini_dt).total_seconds() / 3600, 2))
                                          if r.get('conocimiento_tiempos') == 'Total' else 0)
                            except:
                                dur_u = 0
                            conn.execute(text("""
                                UPDATE incidents SET zona=:z, subzona=:sz, afectacion_general=:ag,
                                    servicio=:s, categoria=:c, equipo_afectado=:e,
                                    inicio_incidente=:idi, fin_incidente=:idf,
                                    clientes_afectados=:cl, causa_raiz=:cr, descripcion=:d,
                                    duracion_horas=:dur, conocimiento_tiempos=:con
                                WHERE id=:id
                            """), {
                                "z": r.get('zona',''),   "sz": r.get('subzona',''),
                                "ag": bool(r.get('afectacion_general', True)),
                                "s": r.get('servicio',''), "c": r.get('categoria',''),
                                "e": r.get('equipo_afectado',''),
                                "idi": ini_dt, "idf": fin_dt,
                                "cl": int(r.get('clientes_afectados', 0)),
                                "cr": r.get('causa_raiz',''),  "d": r.get('descripcion',''),
                                "dur": dur_u, "con": r.get('conocimiento_tiempos','Total'),
                                "id": int(r['id']),
                            })
                log_audit("UPDATE", "Edición masiva de registros.")
                load_data_rango.clear(); st.rerun()

            st.divider()
            st.download_button(
                "📥 Descargar CSV de esta Tabla",
                df_d.drop(columns=drop_cols, errors='ignore').to_csv(index=False).encode(),
                f"NOC_Export_{fecha_ini}_{fecha_fin}.csv", "text/csv",
                use_container_width=True,
            )


# =============================================================================
# --- [ETIQUETA: TAB 3 — CONFIGURACIÓN] ---
# Disponible solo para rol: admin
# Gestión CRUD de catálogos y usuarios.
# =============================================================================
if role == 'admin' and len(tabs) > 3:
    with tabs[3]:
        st.markdown("### ⚙️ Configuración del Sistema")
        st.caption("Administra catálogos y usuarios. Los cambios se reflejan inmediatamente.")

        # Sub-tabs para organizar sin columnas aplastadas
        t_zonas, t_equipos, t_causas, t_usuarios = st.tabs(
            ["🗺️ Zonas", "🖥️ Equipos", "🛠️ Causas", "👤 Usuarios y Accesos"])

        # ── Helper reutilizable para listar + eliminar ──
        def _cat_crud_list(items: list, del_key_prefix: str, tabla: str,
                           col_label: str = "nombre", extra_col: str = ""):
            for item in items:
                name   = item[0] if isinstance(item, tuple) else item
                desc   = str(item[1]) if isinstance(item, tuple) and len(item) > 1 else ""
                c_n, c_d = st.columns([5, 1])
                c_n.text(f"{col_label} {name}{'  —  '+desc if desc else ''}")
                if c_d.button("🗑️", key=f"del_{del_key_prefix}_{name}"):
                    try:
                        with engine.begin() as conn:
                            conn.execute(text(f"DELETE FROM {tabla} WHERE nombre=:n"), {"n": name})
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))

        # ── ZONAS ──
        with t_zonas:
            st.markdown("#### Gestión de Zonas / Nodos")
            zonas_bd = get_zonas()
            for z_nombre, z_lat, z_lon in zonas_bd:
                c_zn, c_zdel = st.columns([5, 1])
                c_zn.text(f"📍 {z_nombre}  (Lat: {z_lat:.4f}, Lon: {z_lon:.4f})")
                if c_zdel.button("🗑️", key=f"del_z_{z_nombre}"):
                    try:
                        with engine.begin() as conn:
                            conn.execute(text("DELETE FROM cat_zonas WHERE nombre=:n"), {"n": z_nombre})
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))
            st.divider()
            with st.form("form_add_zona", clear_on_submit=True):
                st.markdown("**➕ Agregar Nueva Zona**")
                nz   = st.text_input("Nombre del Nodo")
                cla, clo = st.columns(2)
                nlat = cla.number_input("Latitud",  value=13.6929, format="%.4f")
                nlon = clo.number_input("Longitud", value=-89.2182, format="%.4f")
                if st.form_submit_button("Agregar Zona") and nz:
                    try:
                        with engine.begin() as conn:
                            conn.execute(
                                text("INSERT INTO cat_zonas (nombre,lat,lon) VALUES (:n,:la,:lo)"),
                                {"n": nz, "la": nlat, "lo": nlon})
                        st.rerun()
                    except:
                        st.error("Error: Zona duplicada.")

        # ── EQUIPOS ──
        with t_equipos:
            st.markdown("#### Gestión de Equipos de Red")
            for eq in get_cat("cat_equipos"):
                c_en, c_edel = st.columns([5,1])
                c_en.text(f"🖥️ {eq}")
                if c_edel.button("🗑️", key=f"del_eq_{eq}"):
                    try:
                        with engine.begin() as conn:
                            conn.execute(text("DELETE FROM cat_equipos WHERE nombre=:n"), {"n": eq})
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))
            st.divider()
            with st.form("form_add_eq", clear_on_submit=True):
                st.markdown("**➕ Agregar Equipo**")
                ne = st.text_input("Nombre del dispositivo")
                if st.form_submit_button("Agregar Equipo") and ne:
                    try:
                        with engine.begin() as conn:
                            conn.execute(text("INSERT INTO cat_equipos (nombre) VALUES (:n)"), {"n": ne})
                        st.rerun()
                    except:
                        st.error("Error: Equipo duplicado.")

        # ── CAUSAS ──
        with t_causas:
            st.markdown("#### Gestión de Causas Raíz")
            causas_bd = get_causas_con_flag()
            for causa, ext in causas_bd.items():
                c_cn, c_ct, c_cdel = st.columns([4,2,1])
                c_cn.text(f"🛠️ {causa}")
                c_ct.caption("Externa ⚡" if ext else "Interna 🔧")
                if c_cdel.button("🗑️", key=f"del_ca_{causa}"):
                    try:
                        with engine.begin() as conn:
                            conn.execute(text("DELETE FROM cat_causas WHERE nombre=:n"), {"n": causa})
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))
            st.divider()
            with st.form("form_add_causa", clear_on_submit=True):
                st.markdown("**➕ Agregar Causa**")
                nc  = st.text_input("Descripción de la causa")
                nce = st.checkbox("¿Es factor Externo (Terceros, Clima, etc.)?")
                if st.form_submit_button("Agregar Causa") and nc:
                    try:
                        with engine.begin() as conn:
                            conn.execute(
                                text("INSERT INTO cat_causas (nombre,es_externa) VALUES (:n,:e)"),
                                {"n": nc, "e": nce})
                        st.rerun()
                    except:
                        st.error("Error: Causa duplicada.")

        # ── USUARIOS Y AUDIT LOG ──
        with t_usuarios:
            st.markdown("#### Control de Accesos")
            cu, clg = st.columns([1, 2], gap="large")

            with cu:
                with st.form("form_u", clear_on_submit=True):
                    st.markdown("**Crear Usuario**")
                    nu   = st.text_input("Nombre de usuario")
                    np_u = st.text_input("Contraseña", type="password")
                    nrl  = st.selectbox("Rol", ["viewer", "auditor", "admin"])
                    npr  = st.text_input("Pregunta de seguridad (opcional)")
                    nrs  = st.text_input("Respuesta")
                    if st.form_submit_button("➕ Crear Cuenta") and nu and np_u:
                        try:
                            with engine.begin() as conn:
                                conn.execute(text("""
                                    INSERT INTO users (username,password_hash,role,pregunta,respuesta)
                                    VALUES (:u,:h,:r,:p,:rs)
                                """), {"u": nu, "h": hash_pw(np_u), "r": nrl, "p": npr, "rs": nrs})
                            st.toast("✅ Usuario creado exitosamente.")
                            time.sleep(0.4); st.rerun()
                        except:
                            st.toast("❌ Error: Nombre de usuario duplicado.", icon="❌")

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
                            "failed_attempts": "Intentos",
                        },
                        use_container_width=True, hide_index=True,
                    )
                    filas_del   = ed_usrs[ed_usrs["Sel"] == True]
                    hay_cambios = not (df_usrs.drop(columns=['Sel']).reset_index(drop=True)
                                       .equals(ed_usrs.drop(columns=['Sel']).reset_index(drop=True)))

                    u1c, u2c = st.columns(2)
                    if not filas_del.empty and u1c.button("🗑️ Eliminar", use_container_width=True):
                        if "Admin" in filas_del['username'].values:
                            st.error("No se puede eliminar la cuenta Admin raíz.")
                        else:
                            with engine.begin() as conn:
                                for rid in filas_del['id']:
                                    conn.execute(text("DELETE FROM users WHERE id=:id"), {"id": int(rid)})
                            st.rerun()

                    if hay_cambios and u2c.button("💾 Guardar Permisos", type="primary", use_container_width=True):
                        with engine.begin() as conn:
                            for i, er in ed_usrs.iterrows():
                                orig = df_usrs.drop(columns=['Sel']).iloc[i]
                                if not orig.equals(er.drop('Sel')):
                                    conn.execute(text("""
                                        UPDATE users SET role=:r,is_banned=:b,failed_attempts=:f WHERE id=:id
                                    """), {"r": str(er['role']), "b": bool(er['is_banned']),
                                           "f": int(er['failed_attempts']), "id": int(er['id'])})
                        st.rerun()
                except Exception as ex:
                    st.error(str(ex))

            st.divider()

            # --- [ETIQUETA: PANEL SMARTOLT LIVE] ---
            st.markdown("#### 🔴 Estado Live SmartOLT")
            smartolt_data = fetch_smartolt_clients()
            if "_error" in smartolt_data:
                st.error(f"SmartOLT sin conexión: {smartolt_data['_error']}")
                st.caption("Verifique `smartolt_api_url` y `smartolt_api_key` en st.secrets.")
            elif not smartolt_data:
                st.warning("SmartOLT conectado pero sin OLTs registradas.")
            else:
                df_smart = pd.DataFrame(
                    [(k, v) for k, v in smartolt_data.items()],
                    columns=["OLT", "Clientes Activos"],
                ).sort_values("Clientes Activos", ascending=False)
                st.dataframe(df_smart, use_container_width=True, hide_index=True)
                st.caption(f"Total: **{df_smart['Clientes Activos'].sum():,}** clientes activos en {len(df_smart)} OLTs · Cache renovado cada hora.")

            st.divider()
            st.markdown("#### 📜 Audit Log del Sistema")
            try:
                with engine.connect() as conn:
                    logs = pd.read_sql(
                        text("SELECT timestamp AS Fecha, username AS Usuario, action AS Accion, details AS Detalles FROM audit_logs ORDER BY id DESC"),
                        conn,
                    )
                t_lp  = max(1, math.ceil(len(logs) / 10))
                p_log = st.number_input("Página de log", 1, t_lp, 1)
                st.dataframe(logs.iloc[(p_log-1)*10 : p_log*10],
                             use_container_width=True, hide_index=True)
            except Exception as ex:
                st.warning(str(ex))
