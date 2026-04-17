import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px
from datetime import datetime
import calendar
import time

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(
    page_title="Multinet NOC Analytics | Enterprise Operations",
    layout="wide",
    page_icon="🌐"
)

# --- CONEXIÓN SEGURA A NEON ---
@st.cache_resource
def get_engine():
    return create_engine(
        st.secrets["neon_dsn"],
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args={"connect_timeout": 10}
    )

engine = get_engine()

# --- CARGA DE DATOS DESDE NEON ---
@st.cache_data(ttl=300)
def load_data():
    query = "SELECT * FROM incidents ORDER BY id ASC"
    try:
        with engine.connect() as conn:
            conn.execute(text("ROLLBACK"))
            df = pd.read_sql(text(query), conn)
    except Exception:
        engine.dispose()
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)

    # Normalización robusta de fechas y horas
    if "fecha_inicio" in df.columns:
        df["fecha_inicio"] = pd.to_datetime(
            df["fecha_inicio"].astype(str)
                .str.replace("-", "/", regex=False)
                .str.replace(".", "/", regex=False),
            dayfirst=True,
            errors="coerce"
        ).dt.strftime("%d/%m/%Y")

    if "fecha_fin" in df.columns:
        df["fecha_fin"] = pd.to_datetime(
            df["fecha_fin"].astype(str)
                .str.replace("-", "/", regex=False)
                .str.replace(".", "/", regex=False),
            dayfirst=True,
            errors="coerce"
        ).dt.strftime("%d/%m/%Y")

    if "hora_inicio" in df.columns:
        df["hora_inicio"] = df["hora_inicio"].astype(str)

    if "hora_fin" in df.columns:
        df["hora_fin"] = df["hora_fin"].astype(str)

    return df

# --- MOTOR DE CÁLCULOS KPI's ---
def calcular_metricas(df_kpi, horas_mes_total):
    if df_kpi.empty:
        return 0.0, 0.0, 100.0, 0.0, 0, 0.0

    downtime_bruto = df_kpi['duracion_horas'].sum()

    mask_acd = (df_kpi['duracion_horas'] > 0) & (df_kpi['clientes_afectados'] > 0)
    acd = (
        (df_kpi.loc[mask_acd, 'duracion_horas'] * df_kpi.loc[mask_acd, 'clientes_afectados']).sum()
        / df_kpi.loc[mask_acd, 'clientes_afectados'].sum()
    ) if mask_acd.any() else 0.0

    v_kpi = df_kpi[df_kpi['conocimiento_tiempos'] == 'Total'].copy()
    tiempo_real = 0.0
    if not v_kpi.empty:
        s = pd.to_datetime(
            v_kpi['fecha_inicio'].astype(str) + ' ' + v_kpi['hora_inicio'].astype(str),
            errors='coerce', dayfirst=True
        )
        e = pd.to_datetime(
            v_kpi['fecha_fin'].astype(str) + ' ' + v_kpi['hora_fin'].astype(str),
            errors='coerce', dayfirst=True
        )
        m = s.notna() & e.notna() & (s <= e)
        if m.any():
            i = sorted(list(zip(s[m].tolist(), e[m].tolist())), key=lambda x: x[0])
            mg = [list(i[0])]
            for c in i[1:]:
                if c[0] <= mg[-1][1]:
                    mg[-1][1] = max(mg[-1][1], c[1])
                else:
                    mg.append(list(c))
            tiempo_real = sum((end - stp).total_seconds() for stp, end in mg) / 3600.0

    sla_resultante = max(0.0, min(100.0, ((horas_mes_total - tiempo_real) / horas_mes_total) * 100))
    mttr = df_kpi[df_kpi['duracion_horas'] > 0]['duracion_horas'].mean() if not df_kpi[df_kpi['duracion_horas'] > 0].empty else 0.0
    clientes = int(df_kpi['clientes_afectados'].sum())
    max_downt = df_kpi['duracion_horas'].max() if not df_kpi.empty else 0.0

    return downtime_bruto, acd, sla_resultante, mttr, clientes, max_downt

# --- ESTILOS ---
st.markdown("""
<style>
div.stButton > button:first-child {
    background-color: #0068c9;
    color: white;
    border-radius: 8px;
    width: 100%;
    font-weight: 600;
    border: none;
    padding: 0.5rem 1rem;
    transition: all 0.2s;
}
div.stButton > button:first-child:hover {
    background-color: #0056a3;
}
[data-testid="stMetricLabel"] {
    color: #808495 !important;
    font-size: 15px !important;
}
[data-testid="stMetricValue"] {
    color: #ffffff !important;
    font-size: 32px !important;
    font-weight: 700 !important;
}
</style>
""", unsafe_allow_html=True)

meses_nombres = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
                 "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]

# --- SIDEBAR ---
with st.sidebar:
    st.title("🏢 Centro de Operaciones de Red (NOC)")
    st.caption("Panel de Control Gerencial Multinet | v3.3")

    mes_actual_num = datetime.now().month
    mes_seleccionado = st.selectbox("📅 Ciclo de Análisis Mensual", meses_nombres, index=mes_actual_num - 1)

    st.divider()
    st.header("📋 Formulario de Incidencias Operativas")

    zona = st.text_input("📍 Ubicación de la Incidencia (Nodo o Zona)")

    c_serv1, c_serv2 = st.columns([2, 3])
    servicio = c_serv1.selectbox("🌐 Servicio Principal Afectado", ["Internet", "Cable TV (CATV)", "IPTV (Mnet+)"])
    categoria = c_serv2.selectbox("🏢 Segmento de Mercado", ["Red Multinet (Troncal)", "Cliente Corporativo"])

    equipo = st.selectbox("🖥️ Equipamiento de Red Afectado", [
        "OLT","RB/Mikrotik","Switch","ONU","Servidor",
        "Fibra Principal","Caja NAP","Mufa","Splitter",
        "Sistema UNIFI","Antenas Ubiquiti"
    ])

    st.write("---")
    st.write("⏱️ **Registro de Tiempos: Inicio de Falla**")

    c1, c2 = st.columns(2)
    f_i = c1.date_input("📅 Fecha de Inicio")
    conoce_h_i = c1.radio("🕒 ¿Conoce la Hora Exacta?", ["No","Sí"], horizontal=True)

    if conoce_h_i == "Sí":
        h_i = c2.time_input("🕒 Hora de Apertura")
        hora_inicio_final = h_i.strftime("%H:%M:%S")
    else:
        hora_inicio_final = "N/A"

    st.write("---")
    st.write("✅ **Registro de Tiempos: Resolución y Cierre**")

    c_c1, c_c2 = st.columns(2)
    f_f = c_c1.date_input("📅 Fecha de Cierre")
    conoce_h_f = c_c1.radio("🕒 ¿Conoce la Hora Exacta?", ["No","Sí"], horizontal=True)

    if conoce_h_f == "Sí":
        h_f = c_c2.time_input("🕒 Hora de Cierre")
        final_h = h_f.strftime("%H:%M:%S")
    else:
        final_h = "N/A"

    duracion = 0
    if conoce_h_i == "Sí" and conoce_h_f == "Sí":
        desc_conocimiento = "Total"
        try:
            dt_i = datetime.combine(f_i, h_i)
            dt_f = datetime.combine(f_f, h_f)
            duracion = round((dt_f - dt_i).total_seconds() / 3600, 2)
            if duracion < 0:
                duracion = 0
        except:
            duracion = 0
    elif conoce_h_i == "No" and conoce_h_f == "No":
        desc_conocimiento = "Parcial (Solo Fechas)"
    elif conoce_h_i == "Sí" and conoce_h_f == "No":
        desc_conocimiento = "Parcial (Falta Hora Cierre)"
    else:
        desc_conocimiento = "Parcial (Falta Hora Inicio)"

    if categoria == "Cliente Corporativo":
        clientes_form = 1
    else:
        clientes_form = st.number_input("👤 Cantidad de Clientes Afectados", min_value=0, step=1)

    causa = st.selectbox("🛠️ Diagnóstico Técnico (Causa Raíz)", [
        "Corte de Fibra por Terceros","Corte de Fibra (No Especificado)",
        "Caída de Árboles sobre Fibra","Falla de Energía Comercial",
        "Corrosión en Equipos","Daños por Fauna","Falla de Hardware",
        "Falla de Configuración","Falla de Redundancia","Saturación de Tráfico",
        "Saturación en Servidor UNIFI","Falla de Inicio en UNIFI",
        "Mantenimiento Programado","Vandalismo o Hurto","Condiciones Climáticas"
    ])

    desc = st.text_area("📝 Descripción Técnica y Detallada del Incidente")

    if st.button("💾 Guardar Registro Operativo"):
        insert_sql = text("""
            INSERT INTO incidents (
                worksheet_name, gsheet_id, zona, servicio, categoria, equipo_afectado,
                fecha_inicio, hora_inicio, fecha_fin, hora_fin,
                clientes_afectados, causa_raiz, descripcion,
                duracion_horas, conocimiento_tiempos
            ) VALUES (
                :ws_name, 0, :zona, :servicio, :categoria, :equipo,
                :fecha_inicio, :hora_inicio, :fecha_fin, :hora_fin,
                :clientes, :causa, :descripcion,
                :duracion, :conocimiento
            )
        """)
        nombre_pestana = f"{meses_nombres[f_i.month - 1]} {f_i.year}"
        try:
            with engine.begin() as conn:
                conn.execute(insert_sql, {
                    "ws_name": nombre_pestana,
                    "zona": zona,
                    "servicio": servicio,
                    "categoria": categoria,
                    "equipo": equipo,
                    "fecha_inicio": f_i.strftime("%d/%m/%Y"),
                    "hora_inicio": hora_inicio_final,
                    "fecha_fin": f_f.strftime("%d/%m/%Y"),
                    "hora_fin": final_h,
                    "clientes": int(clientes_form),
                    "causa": causa,
                    "descripcion": desc,
                    "duracion": duracion,
                    "conocimiento": desc_conocimiento
                })
            st.toast(f"Registro almacenado en '{nombre_pestana}'.")
            time.sleep(1)
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"Error al guardar: {e}")

# --- PROCESAMIENTO ---
try:
    df_total = load_data()

    if df_total.empty:
        st.info("No hay datos en la base de datos todavía.")
        st.stop()

    df_total.columns = [c.lower() for c in df_total.columns]

    df_total['fecha_convertida'] = pd.to_datetime(
        df_total['fecha_inicio'].astype(str)
            .str.replace("-", "/", regex=False)
            .str.replace(".", "/", regex=False),
        dayfirst=True,
        errors="coerce"
    )

    df_total['mes_nombre'] = df_total['fecha_convertida'].dt.month.map(
        lambda x: meses_nombres[int(x) - 1] if pd.notnull(x) else None
    )

    df_total['duracion_horas'] = pd.to_numeric(df_total['duracion_horas'], errors='coerce').fillna(0)
    df_total['clientes_afectados'] = pd.to_numeric(df_total['clientes_afectados'], errors='coerce').fillna(0).astype(int)

    df_mes = df_total[df_total['mes_nombre'] == mes_seleccionado].copy()

    st.title(f"📊 Dashboard Operacional NOC: {mes_seleccionado} {datetime.now().year}")

    if not df_mes.empty:
        df_filtrado = df_mes.copy()

        mes_index = meses_nombres.index(mes_seleccionado) + 1
        anio_actual = datetime.now().year
        dias_mes = calendar.monthrange(anio_actual, mes_index)[1]
        horas_totales_mes = dias_mes * 24

        downtime_total, acd_horas, sla_porcentaje, avg_mttr, cl_imp, max_h = calcular_metricas(df_filtrado, horas_totales_mes)
        dias_totales = downtime_total / 24.0

        # KPIs
        st.write("---")
        k1, k2, k3 = st.columns(3)
        k1.metric("⏱️ MTTR", f"{avg_mttr:.2f} h")
        k2.metric("👥 Clientes Afectados", f"{cl_imp}")
        k3.metric("⏳ Impacto", f"{dias_totales:.2f} días")

        st.write("")
        k4, k5, k6 = st.columns(3)
        k4.metric("🛑 Falla Más Larga", f"{max_h:.2f} h")
        k5.metric("📈 SLA", f"{sla_porcentaje:.2f}%")
        k6.metric("📉 ACD", f"{acd_horas:.2f} h/cliente")

        # GRÁFICAS
        st.divider()
        st.subheader("📈 Análisis Visual")

        col_g1, col_g2 = st.columns(2)

        causas_cortas = {
            "Corte de Fibra por Terceros": "Terceros",
            "Corte de Fibra (No Especificado)": "Fibra",
            "Caída de Árboles sobre Fibra": "Árboles",
            "Falla de Energía Comercial": "Energía",
            "Corrosión en Equipos": "Corrosión",
            "Daños por Fauna": "Fauna",
            "Falla de Hardware": "Hardware",
            "Falla de Configuración": "Config",
            "Falla de Redundancia": "Redundancia",
            "Saturación de Tráfico": "Saturación",
            "Saturación en Servidor UNIFI": "Sat UNIFI",
            "Falla de Inicio en UNIFI": "Inicio UNIFI",
            "Mantenimiento Programado": "Mantenimiento",
            "Vandalismo o Hurto": "Vandalismo",
            "Condiciones Climáticas": "Clima"
        }

        df_caus = df_filtrado.groupby('causa_raiz').size().reset_index(name='Alertas')
        df_caus['Causa_Corta'] = df_caus['causa_raiz'].map(lambda x: causas_cortas.get(x, x))

        fig_rca = px.pie(df_caus, names='Causa_Corta', values='Alertas', hole=0.5, template="plotly_dark")
        col_g1.plotly_chart(fig_rca, use_container_width=True)

        df_req = df_filtrado.groupby('equipo_afectado').size().reset_index(name='Fallos')
        fig_eq = px.bar(df_req, x='Fallos', y='equipo_afectado', orientation='h', template="plotly_dark")
        col_g2.plotly_chart(fig_eq, use_container_width=True)

        st.write("")
        col_g3, col_g4 = st.columns(2)

        df_serv = df_filtrado.groupby('servicio').size().reset_index(name='Total_Eventos')
        fig_serv = px.bar(df_serv, x='Total_Eventos', y='servicio', orientation='h', template="plotly_dark")
        col_g3.plotly_chart(fig_serv, use_container_width=True)

        top_zonas = df_filtrado.groupby('zona')['duracion_horas'].sum().nlargest(5).reset_index()
        fig_bar_zonas = px.bar(top_zonas, x='duracion_horas', y='zona', orientation='h', template="plotly_dark")
        col_g4.plotly_chart(fig_bar_zonas, use_container_width=True)

        # Matriz de Riesgo
        st.write("---")
        df_riesgo = df_filtrado.groupby('zona').agg(
            Frecuencia=('id', 'count'),
            Horas_Down=('duracion_horas', 'sum'),
            Afect_Totales=('clientes_afectados', 'sum')
        ).reset_index()

        if not df_riesgo.empty:
            fig_sc = px.scatter(df_riesgo, x='Frecuencia', y='Horas_Down', size='Afect_Totales', color='zona', template="plotly_dark")
            st.plotly_chart(fig_sc, use_container_width=True)

        # Tendencia diaria
