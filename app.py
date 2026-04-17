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
        return df
    except Exception:
        engine.dispose()
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df

# --- MOTOR DE CÁLCULOS KPI's ESTRATÉGICOS ---
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
            errors='coerce'
        )
        e = pd.to_datetime(
            v_kpi['fecha_fin'].astype(str) + ' ' + v_kpi['hora_fin'].astype(str),
            errors='coerce'
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


# --- ESTILOS PROFESIONALES ---
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
        border: none;
        color: white;
    }
    [data-testid="stMetricLabel"] {
        color: #808495 !important;
        font-size: 15px !important;
        font-weight: 500 !important;
    }
    [data-testid="stMetricValue"] {
        color: #ffffff !important;
        font-size: 32px !important;
        font-weight: 700 !important;
    }
    .stAlert, .stMarkdown { border-radius: 8px; }

    /* Botón de eliminar en rojo */
    div[data-testid="stButton-delete"] > button:first-child {
        background-color: #c0392b !important;
    }
    div[data-testid="stButton-delete"] > button:first-child:hover {
        background-color: #a93226 !important;
    }

    /* Botón de guardar en verde */
    div[data-testid="stButton-save"] > button:first-child {
        background-color: #27ae60 !important;
    }
    div[data-testid="stButton-save"] > button:first-child:hover {
        background-color: #1e8449 !important;
    }
    
    /* Corrección de contraste global para la tabla */
    :root {
        --primary-color: #0068c9;
    }
    [data-testid="stDataFrame"] {
        --text-color: #ffffff !important;
    }
    </style>
    """, unsafe_allow_html=True)

meses_nombres = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
                 "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]

# --- MAPA: DICCIONARIO DE COORDENADAS ---
COORDS_ZONAS = {
    "Papaya Garden": {"lat": 13.4925, "lon": -89.3822},
    "La Libertad - Conchalio": {"lat": 13.4900, "lon": -89.3245},
    "La Libertad - Julupe": {"lat": 13.5011, "lon": -89.3300},
    "Costa del Sol": {"lat": 13.3039, "lon": -88.9450},
    "OLT ARG": {"lat": 13.4880, "lon": -89.3200},
    "La Libertad - Agroferreteria": {"lat": 13.4905, "lon": -89.3210},
    "Servidores Gabriela Mistral": {"lat": 13.7000, "lon": -89.2000},
    "Los Blancos": {"lat": 13.3100, "lon": -88.9200},
    "Zaragoza": {"lat": 13.5850, "lon": -89.2890}
}
COORD_DEFAULT = {"lat": 13.6929, "lon": -89.2182} # Centro por defecto

# --- SIDEBAR: GESTIÓN OPERATIVA ---
with st.sidebar:
    st.title("🏢 Centro de Operaciones de Red (NOC)")
    st.caption("Panel de Control Gerencial Multinet | Enterprise v4.0")

    mes_actual_num = datetime.now().month
    mes_seleccionado = st.selectbox("📅 Ciclo de Análisis Mensual", meses_nombres, index=mes_actual_num - 1)

    st.divider()
    st.markdown("### ⚙️ Herramientas de NOC")
    auto_refresh = st.toggle("🔄 Modo TV (Auto-Refresh 60s)")
    
    if auto_refresh:
        # Esto inyecta un código HTML seguro que recarga la página cada 60s sin bloquear Python
        import streamlit.components.v1 as components
        components.html("<meta http-equiv='refresh' content='60'>", height=0)
        st.caption("Auto-Refresh Activado. Pantalla actualizándose automáticamente.")
    
    st.write("---")
    st.info("Navegue por las pestañas superiores para ingresar incidencias o realizar auditorías.", icon="👆")

# --- PROCESAMIENTO Y ANALÍTICA DE DATOS ---
df_total = pd.DataFrame()
try:
    df_total = load_data()
    if not df_total.empty:
        df_total.columns = [c.lower() for c in df_total.columns]
        df_total['fecha_convertida'] = pd.to_datetime(df_total['fecha_inicio'], errors='coerce')
        df_total['mes_nombre'] = df_total['fecha_convertida'].dt.month.map(
            lambda x: meses_nombres[int(x) - 1] if pd.notnull(x) else None
        )
        df_total['duracion_horas'] = pd.to_numeric(df_total['duracion_horas'], errors='coerce').fillna(0)
        df_total['clientes_afectados'] = pd.to_numeric(df_total['clientes_afectados'], errors='coerce').fillna(0).astype(int)
except Exception as e:
    st.error(f"⚠️ Error al conectar con Neon DB: {e}")

df_mes = pd.DataFrame()
if not df_total.empty:
    df_mes = df_total[df_total['mes_nombre'] == mes_seleccionado].copy()

# ============================================================
# --- PESTAÑAS PRINCIPALES ---
# ============================================================
tab1, tab2, tab3 = st.tabs(["📊 Dashboard de Monitoreo", "📝 Formulario de Registro", "🔐 Panel de Auditoría y BD"])

# -------------------------------------------------------------
# TAB 1: DASHBOARD
# -------------------------------------------------------------
with tab1:
    st.title(f"Dashboard Operacional NOC: {mes_seleccionado} {datetime.now().year}")

    if df_total.empty:
        st.info("No hay datos en la base de datos todavía. Registre incidencias en la pestaña correspondiente.")
    elif df_mes.empty:
        st.success(f"🟢 Excelente estado operativo: No se detectan fallas mayores registradas para el ciclo mensual de {mes_seleccionado}.")
    else:
        df_filtrado = df_mes.copy()

        # --- INDICADOR DE ESTADO EN TIEMPO REAL ---
        fallas_activas = df_filtrado[(df_filtrado['hora_fin'].isnull()) | (df_filtrado['hora_fin'] == '') | (df_filtrado['hora_fin'] == 'None')]
        if fallas_activas.empty:
            st.success("🟢 **ESTADO DE LA RED: NORMAL** - Todas las incidencias de este mes han sido cerradas.", icon="🟢")
        else:
            zonas_criticas = ", ".join(fallas_activas['zona'].astype(str).unique())
            st.error(f"🔴 **ESTADO DE LA RED: CRÍTICO** - Hay {len(fallas_activas)} alerta(s) activa(s) sin resolución en: {zonas_criticas}.", icon="🔴")

        mes_index = meses_nombres.index(mes_seleccionado) + 1
        anio_actual = datetime.now().year
        dias_mes = calendar.monthrange(anio_actual, mes_index)[1]
        horas_totales_mes = dias_mes * 24

        downtime_total, acd_horas, sla_porcentaje, avg_mttr, cl_imp, max_h = calcular_metricas(df_filtrado, horas_totales_mes)
        dias_totales = downtime_total / 24.0

        delta_m, delta_a, delta_s, delta_dias = None, None, None, None

        if mes_index > 1:
            mes_pasado_nom = meses_nombres[mes_index - 2]
            dias_pasado = calendar.monthrange(anio_actual, mes_index - 1)[1]
            df_pasado = df_total[df_total['mes_nombre'] == mes_pasado_nom].copy()

            if not df_pasado.empty:
                d_b_p, acd_p, sla_p, mttr_p, _, _ = calcular_metricas(df_pasado, dias_pasado * 24)
                if mttr_p > 0: delta_m = f"{avg_mttr - mttr_p:+.1f} horas"
                if acd_p > 0: delta_a = f"{acd_horas - acd_p:+.1f} horas"
                if sla_p > 0: delta_s = f"{sla_porcentaje - sla_p:+.2f}%"
                if d_b_p > 0:
                    dias_p = d_b_p / 24.0
                    delta_dias = f"{dias_totales - dias_p:+.1f} días"

        # --- KPIs ---
        st.write("---")
        k1, k2, k3 = st.columns(3)
        k1.metric("⏱️ Tiempo Promedio de Resolución (MTTR)", f"{avg_mttr:.2f} horas", delta=delta_m, delta_color="inverse",
                  help="Promedio de horas empleadas para reparar el servicio tras la notificación de la falla.")
        k2.metric("👥 Total de Clientes Interrumpidos", f"{cl_imp} clientes",
                  help="Cantidad consolidada de usuarios que experimentaron cortes de servicio.")
        k3.metric("⏳ Impacto Operativo Acumulado", f"{dias_totales:.2f} días", delta=delta_dias, delta_color="inverse",
                  help="Sumatoria global del tiempo de desconexión expresado en la equivalencia de días enteros.")

        st.write("")
        k4, k5, k6 = st.columns(3)
        k4.metric("🛑 Duración de la Falla Más Crítica", f"{max_h:.2f} horas",
                  help="La duración en horas del incidente más severo y prolongado registrado en este periodo mensual.")
        k5.metric("📈 Porcentaje de Disponibilidad (SLA)", f"{sla_porcentaje:.2f}%", delta=delta_s, delta_color="normal",
                  help="Nivel integral de servicio operativo (SLA) basado en las horas del mes.")
        k6.metric("📉 Promedio de Afectación por Cliente (ACD)", f"{acd_horas:.2f} horas / cliente", delta=delta_a, delta_color="inverse",
                  help="Promedio estadístico de horas continuas en las que un cliente experimentó interrupción de servicio.")

        st.caption("ℹ️ **Nota sobre Clientes Afectados:** La cantidad de clientes mostrada es una estimación. Cuando no se cuenta con el dato exacto, el sistema usa un valor base para no alterar los promedios.")

        # --- ESTILOS COMPARTIDOS DE ALTO CONTRASTE PARA GRAFICAS ---
        hover_style = dict(bgcolor="#1c1c1c", font_size=14, font_color="#ffffff", bordercolor="#4a4a4a")

        # --- GRÁFICAS ---
        st.divider()
        st.subheader("📈 Análisis Visual del Rendimiento Operativo")

        # --- NUEVO: MAPA GEOGRÁFICO ---
        df_mapa = df_filtrado.copy()
        df_mapa['lat'] = df_mapa['zona'].apply(lambda x: COORDS_ZONAS.get(x, COORD_DEFAULT)['lat'])
        df_mapa['lon'] = df_mapa['zona'].apply(lambda x: COORDS_ZONAS.get(x, COORD_DEFAULT)['lon'])
        
        df_map_agg = df_mapa.groupby(['zona', 'lat', 'lon']).agg(
            Frecuencia=('id', 'count'), Horas_Down=('duracion_horas', 'sum'), Clientes=('clientes_afectados', 'sum')
        ).reset_index()

        fig_map = px.scatter_mapbox(
            df_map_agg, lat="lat", lon="lon", hover_name="zona",
            hover_data={"lat": False, "lon": False, "Frecuencia": True, "Horas_Down": True, "Clientes": True},
            size="Clientes", color="Horas_Down", color_continuous_scale="Reds", 
            zoom=9.5, mapbox_style="carto-darkmatter", title="🗺️ <b>Mapa de Calor Geográfico de Afectaciones</b>"
        )
        fig_map.update_layout(margin=dict(l=0, r=0, t=50, b=0), hoverlabel=hover_style)
        st.plotly_chart(fig_map, use_container_width=True, theme=None)
        st.write("")

        col_g1, col_g2 = st.columns(2)

        causas_cortas = {
            "Corte de Fibra por Terceros": "Terceros", "Corte de Fibra (No Especificado)": "Fibra",
            "Caída de Árboles sobre Fibra": "Árboles", "Falla de Energía Comercial": "Energía",
            "Corrosión en Equipos": "Corrosión", "Daños por Fauna": "Fauna",
            "Falla de Hardware": "Hardware", "Falla de Configuración": "Configuración",
            "Falla de Redundancia": "Redundancia", "Saturación de Tráfico": "Saturación",
            "Saturación en Servidor UNIFI": "Sat. UNIFI", "Falla de Inicio en UNIFI": "Inic. UNIFI",
            "Mantenimiento Programado": "Mantenimiento", "Vandalismo o Hurto": "Vandalismo",
            "Condiciones Climáticas": "Clima", "No Especificado": "N/E"
        }

        df_caus = df_filtrado.groupby('causa_raiz').size().reset_index(name='Alertas')
        df_caus['Causa_Corta'] = df_caus['causa_raiz'].map(lambda x: causas_cortas.get(x, str(x).split()[0]))

        fig_rca = px.pie(df_caus, names='Causa_Corta', values='Alertas', hole=0.5,
                         title="🔍 <b>Causas Principales de las Fallas Registradas</b>")
        fig_rca.update_traces(textposition='outside', textinfo='percent+label')
        fig_rca.update_layout(showlegend=False, margin=dict(l=0, r=0, t=60, b=0), hoverlabel=hover_style)
        col_g1.plotly_chart(fig_rca, use_container_width=True, theme=None)

        df_req = df_filtrado.groupby('equipo_afectado').size().reset_index(name='Fallos').sort_values('Fallos', ascending=True)
        fig_eq = px.bar(df_req, x='Fallos', y='equipo_afectado', orientation='h', color='Fallos',
                        title="🛠️ <b>Fallas Acumuladas por Tipo de Equipamiento</b>",
                        color_continuous_scale="Reds")
        fig_eq.update_layout(coloraxis_showscale=False, margin=dict(l=0, r=0, t=60, b=0),
                             xaxis_title="Cantidad de Eventos (Fallas)", yaxis_title="", hoverlabel=hover_style)
        col_g2.plotly_chart(fig_eq, use_container_width=True, theme=None)

        st.write("")
        col_g3, col_g4 = st.columns(2)

        df_serv = df_filtrado.groupby('servicio').size().reset_index(name='Total_Eventos')
        fig_serv_kpi = px.bar(df_serv, x='Total_Eventos', y='servicio', orientation='h',
                              title="🌐 <b>Volumen de Incidencias según el Servicio</b>",
                              text_auto=True, color='servicio',
                              color_discrete_sequence=['#0068c9', '#ff9f43', '#27ae60'])
        fig_serv_kpi.update_layout(showlegend=False, margin=dict(l=0, r=0, t=60, b=0),
                                   xaxis_title="Total de Caídas Registradas", yaxis_title="", hoverlabel=hover_style)
        fig_serv_kpi.update_traces(textfont_size=14, marker_line_width=0)
        col_g3.plotly_chart(fig_serv_kpi, use_container_width=True, theme=None)

        top_zonas = df_filtrado.groupby('zona')['duracion_horas'].sum().nlargest(5).reset_index()
        top_zonas.columns = ['Zona', 'Horas Offline']
        top_zonas['Etiqueta'] = top_zonas['Horas Offline'].apply(lambda x: f"{x:.2f} horas")
        fig_bar_zonas = px.bar(top_zonas, x='Horas Offline', y='Zona', orientation='h',
                               title="📉 <b>Impacto por Zona (Horas sin Servicio)</b>",
                               text='Etiqueta', color='Horas Offline',
                               color_continuous_scale='Blues')
        fig_bar_zonas.update_layout(coloraxis_showscale=False, yaxis={'categoryorder': 'total ascending'},
                                    margin=dict(l=0, r=0, t=60, b=0),
                                    xaxis_title="Total de Horas sin Servicio", yaxis_title="", hoverlabel=hover_style)
        fig_bar_zonas.update_traces(marker_line_width=0, textfont_size=13)
        col_g4.plotly_chart(fig_bar_zonas, use_container_width=True, theme=None)

        # Matriz de Riesgo
        st.write("---")
        df_riesgo = df_filtrado.groupby('zona').agg(
            Frecuencia=('id', 'count'),
            Horas_Down=('duracion_horas', 'sum'),
            Afect_Totales=('clientes_afectados', 'sum')
        ).reset_index()

        if not df_riesgo.empty and df_riesgo['Afect_Totales'].sum() > 0:
            fig_sc = px.scatter(df_riesgo, x='Frecuencia', y='Horas_Down', size='Afect_Totales', color='zona',
                                title="📍 <b>Matriz de Riesgo: Desempeño Crítico por Zonas de Cobertura</b><br>"
                                      "<sup>Nodos en el eje superior presentan tiempos de resolución prolongados. "
                                      "Nodos hacia la derecha sufren fallas recurrentes. "
                                      "El radio del círculo representa el volumen de clientes afectados.</sup>",
                                labels={'Frecuencia': 'Cantidad de Fallas Registradas',
                                        'Horas_Down': 'Horas Totales Caídas (Acumulado)'})
            fig_sc.update_layout(margin=dict(l=0, r=0, t=70, b=0), showlegend=True, hoverlabel=hover_style)
            st.plotly_chart(fig_sc, use_container_width=True, theme=None)

        # Tendencia Diaria
        st.write("")
        df_trend = df_filtrado.groupby('fecha_convertida').size().reset_index(name='Total_Eventos')
        fig_trend = px.area(df_trend, x='fecha_convertida', y='Total_Eventos',
                            title="📅 <b>Tendencia Diaria de Cortes Operativos</b><br>"
                                  "<sup>Muestra la fluctuación y volumen de las incidencias registradas día a día durante este ciclo.</sup>",
                            labels={'fecha_convertida': 'Fechas del Mes', 'Total_Eventos': 'Cantidad Total de Fallas'})
        fig_trend.update_traces(line_color='#0068c9', fillcolor='rgba(0, 104, 201, 0.2)')
        fig_trend.update_layout(hoverlabel=hover_style)
        st.plotly_chart(fig_trend, use_container_width=True, theme=None)

        # --- Exportación Final ---
        st.divider()
        export_cols = [c for c in df_filtrado.columns if c not in ['fecha_convertida', 'mes_nombre', 'id', 'gsheet_id', 'worksheet_name', 'lat', 'lon']]
        csv_m = df_filtrado[export_cols].to_csv(index=False).encode('utf-8')
        st.download_button("📥 Exportar Análisis del Mes a formato Excel (CSV)", data=csv_m,
                           file_name="Reporte_Directivo_NOC.csv", mime='text/csv')

# -------------------------------------------------------------
# TAB 2: FORMULARIO DE REGISTRO AMPLIADO
# -------------------------------------------------------------
with tab2:
    st.header("📝 Formulario de Registro Operativo")
    st.markdown("Ingrese los detalles de la incidencia técnica. Los datos se guardarán directamente en la base de datos.")

    with st.container(border=True):
        zona = st.text_input("📍 Ubicación de la Incidencia (Nodo o Zona)")

        c_serv1, c_serv2 = st.columns(2)
        servicio = c_serv1.selectbox("🌐 Servicio Principal Afectado", ["Internet", "Cable TV (CATV)", "IPTV (Mnet+)"])
        categoria = c_serv2.selectbox("🏢 Segmento de Mercado", ["Red Multinet (Troncal)", "Cliente Corporativo"])

        equipo = st.selectbox("🖥️ Equipamiento de Red Afectado", [
            "OLT", "RB/Mikrotik", "Switch", "ONU", "Servidor",
            "Fibra Principal", "Caja NAP", "Mufa", "Splitter",
            "Sistema UNIFI", "Antenas Ubiquiti"
        ])

        st.write("---")
        col_f1, col_f2 = st.columns(2)
        
        with col_f1:
            st.markdown("#### ⏱️ Tiempos: Inicio de Falla")
            f_i = st.date_input("📅 Fecha de Inicio")
            conoce_h_i = st.radio("🕒 ¿Conoce la Hora Exacta de Inicio?", ["No", "Sí"], horizontal=True)
            if conoce_h_i == "Sí":
                h_i = st.time_input("🕒 Hora de Apertura")
                hora_inicio_final = h_i.strftime("%H:%M:%S")
            else:
                hora_inicio_final = None
                st.info("ℹ️ Se omitirá la hora y la duración será 0.")

        with col_f2:
            st.markdown("#### ✅ Tiempos: Resolución y Cierre")
            f_f = st.date_input("📅 Fecha de Cierre")
            conoce_h_f = st.radio("🕒 ¿Conoce la Hora Exacta de Cierre?", ["No", "Sí"], horizontal=True)
            if conoce_h_f == "Sí":
                h_f = st.time_input("🕒 Hora de Cierre")
                final_h = h_f.strftime("%H:%M:%S")
            else:
                final_h = None
                st.info("ℹ️ Se omitirá la hora y la duración será 0.")

        duracion = 0
        if conoce_h_i == "Sí" and conoce_h_f == "Sí":
            desc_conocimiento = "Total"
            try:
                dt_i = datetime.combine(f_i, h_i)
                dt_f = datetime.combine(f_f, h_f)
                duracion = round((dt_f - dt_i).total_seconds() / 3600, 2)
                if duracion < 0:
                    st.error("⚠️ Error: La fecha y hora de cierre no pueden ser anteriores a las de inicio.")
                    duracion = 0
            except:
                duracion = 0
        elif conoce_h_i == "No" and conoce_h_f == "No":
            desc_conocimiento = "Parcial (Solo Fechas)"
        elif conoce_h_i == "Sí" and conoce_h_f == "No":
            desc_conocimiento = "Parcial (Falta Hora Cierre)"
        else:
            desc_conocimiento = "Parcial (Falta Hora Inicio)"

        st.write("---")
        c_af1, c_af2 = st.columns(2)
        with c_af1:
            if categoria == "Cliente Corporativo":
                clientes_form = 1
                st.info("🏢 Segmento Corporativo: Se contabiliza 1 enlace afectado.")
            else:
                clientes_form = st.number_input("👤 Clientes Afectados (Unidades)", min_value=0, step=1)

        with c_af2:
            causa = st.selectbox("🛠️ Diagnóstico Técnico (Causa Raíz)", [
                "Corte de Fibra por Terceros", "Corte de Fibra (No Especificado)", "Caída de Árboles sobre Fibra",
                "Falla de Energía Comercial", "Corrosión en Equipos", "Daños por Fauna", "Falla de Hardware",
                "Falla de Configuración", "Falla de Redundancia", "Saturación de Tráfico", "Saturación en Servidor UNIFI",
                "Falla de Inicio en UNIFI", "Mantenimiento Programado", "Vandalismo o Hurto", "Condiciones Climáticas"
            ])
            
        desc = st.text_area("📝 Descripción Técnica y Detallada del Incidente")

        if st.button("💾 Guardar Registro Operativo", type="primary"):
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
                        "ws_name": nombre_pestana, "zona": zona, "servicio": servicio, "categoria": categoria,
                        "equipo": equipo, "fecha_inicio": f_i.strftime("%Y-%m-%d"), "hora_inicio": hora_inicio_final,
                        "fecha_fin": f_f.strftime("%Y-%m-%d"), "hora_fin": final_h, "clientes": int(clientes_form),
                        "causa": causa, "descripcion": desc, "duracion": duracion, "conocimiento": desc_conocimiento
                    })
                st.success(f"✅ Información almacenada exitosamente en el periodo '{nombre_pestana}'.")
                time.sleep(1.5)
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Error al guardar: {e}")

# -------------------------------------------------------------
# TAB 3: PANEL DE AUDITORÍA Y BASE DE DATOS
# -------------------------------------------------------------
with tab3:
    st.header("🗂️ Panel Central de Auditoría de Datos")
    
    if df_total.empty:
        st.info("La base de datos está vacía.")
    else:
        st.subheader(f"Incidencias del mes: {mes_seleccionado}")
        busqueda = st.text_input("🔎 Buscar en registros activos:", placeholder="Escriba aquí para filtrar la tabla...")

        cols_to_drop = [c for c in ['fecha_convertida', 'mes_nombre', 'lat', 'lon'] if c in df_filtrado.columns]
        conoce_opciones = ["Total", "Parcial (Solo Fechas)", "Parcial (Falta Hora Cierre)",
                           "Parcial (Falta Hora Inicio)", "Parcial (Solo Fecha)", "Parcial (Solo Hora)", "Ninguno"]
        servicio_opciones = ["Internet", "Cable TV (CATV)", "IPTV (Mnet+)"]

        df_display = df_filtrado.copy()
        if busqueda:
            mask = df_display.astype(str).apply(lambda x: x.str.contains(busqueda, case=False, na=False)).any(axis=1)
            df_display = df_display[mask]

        df_display.insert(0, "Seleccionar", False)

        edited_df = st.data_editor(
            df_display.drop(columns=cols_to_drop, errors='ignore'),
            column_config={
                "Seleccionar": st.column_config.CheckboxColumn("✔ Sel.", default=False),
                "id": None, "worksheet_name": None, "gsheet_id": None,
                "servicio": st.column_config.SelectboxColumn("Servicio", options=servicio_opciones, required=True),
                "fecha_inicio": st.column_config.DateColumn("F. Inicio", format="DD/MM/YYYY"),
                "hora_inicio": st.column_config.TimeColumn("H. Inicio"),
                "fecha_fin": st.column_config.DateColumn("F. Cierre", format="DD/MM/YYYY"),
                "hora_fin": st.column_config.TimeColumn("H. Cierre"),
                "duracion_horas": st.column_config.NumberColumn("Duración (Horas)", disabled=True, format="%.2f"),
                "conocimiento_tiempos": st.column_config.SelectboxColumn("Precisión", options=conoce_opciones, required=True)
            },
            use_container_width=True, hide_index=True, num_rows="fixed", key="main_editor"
        )

        filas_para_eliminar = edited_df[edited_df["Seleccionar"] == True]
        original_data = df_display.drop(columns=cols_to_drop + ['Seleccionar'], errors='ignore')
        edited_data = edited_df.drop(columns=['Seleccionar'])
        hay_cambios = not original_data.reset_index(drop=True).equals(edited_data.reset_index(drop=True))
        hay_seleccion = not filas_para_eliminar.empty

        if hay_seleccion or hay_cambios:
            st.divider()
            st.markdown("### 🔐 Validar y Ejecutar Cambios")

            with st.container(border=True):
                col_auth, col_info = st.columns([2, 3])

                with col_info:
                    st.markdown("#### Resumen de Acciones Solicitadas")
                    if hay_seleccion and hay_cambios:
                        st.warning(f"Ha marcado **{len(filas_para_eliminar)} registro(s)** para eliminación **y** ha modificado campos. Irreversible.", icon="⚠️")
                    elif hay_seleccion:
                        st.error(f"Se borrarán definitivamente **{len(filas_para_eliminar)} registro(s)**.", icon="🚨")
                    elif hay_cambios:
                        st.info("**Modificaciones pendientes:** Las métricas se recalcularán automáticamente.", icon="ℹ️")

                with col_auth:
                    st.markdown("#### Autorización Requerida")
                    pin_ingresado = st.text_input("🔑 PIN Administrador:", type="password", key="pin_acceso")

                acceso_autorizado = (pin_ingresado == "1010")

                if not acceso_autorizado and pin_ingresado != "":
                    st.error("🛑 PIN incorrecto. Acceso denegado.", icon="🛑")
                
                if acceso_autorizado:
                    st.write("---")
                    col_btn_elimin, col_btn_guarda = st.columns(2)

                    if hay_seleccion:
                        with col_btn_elimin:
                            if st.button(f"🗑️ Eliminar ({len(filas_para_eliminar)})", key="btn_eliminar", type="primary", use_container_width=True):
                                ids_eliminar = [int(rid) for rid in filas_para_eliminar['id'].tolist()]
                                try:
                                    with engine.begin() as conn:
                                        for rid in ids_eliminar:
                                            conn.execute(text("DELETE FROM incidents WHERE id = :id"), {"id": rid})
                                    st.success("✅ Eliminados de forma definitiva.")
                                    time.sleep(1)
                                    st.cache_data.clear()
                                    st.rerun()
                                except Exception as e: st.error(f"Error: {e}")

                    if hay_cambios:
                        btn_col = col_btn_guarda if hay_seleccion else col_btn_elimin
                        with btn_col:
                            if st.button("💾 Guardar Modificaciones", key="btn_guardar", type="primary", use_container_width=True):
                                try:
                                    with engine.begin() as conn:
                                        for i in range(len(original_data)):
                                            orig_row = original_data.iloc[i]
                                            edit_row = edited_data.iloc[i]
                                            if not orig_row.equals(edit_row):
                                                f_i_s = str(edit_row.get('fecha_inicio', ''))
                                                h_i_s = str(edit_row.get('hora_inicio', ''))
                                                f_f_s = str(edit_row.get('fecha_fin', ''))
                                                h_f_s = str(edit_row.get('hora_fin', ''))
                                                conoce_s = str(edit_row.get('conocimiento_tiempos', ''))
                                                dur_r = 0.0
                                                try:
                                                    if conoce_s == "Total" and h_i_s not in ["None", "N/A", "NaT", ""] and h_f_s not in ["None", "N/A", "NaT", ""]:
                                                        dt_ini = datetime.strptime(f"{f_i_s} {h_i_s}", "%Y-%m-%d %H:%M:%S")
                                                        dt_fin = datetime.strptime(f"{f_f_s} {h_f_s}", "%Y-%m-%d %H:%M:%S")
                                                        dur_r = max(0.0, round((dt_fin - dt_ini).total_seconds() / 3600, 2))
                                                except: dur_r = 0.0

                                                row_id = int(edit_row.get('id') if edit_row.get('id') is not None else orig_row.get('id'))
                                                sql_hi = None if h_i_s in ["None", "N/A", "NaT", ""] else h_i_s
                                                sql_hf = None if h_f_s in ["None", "N/A", "NaT", ""] else h_f_s

                                                conn.execute(text("""
                                                    UPDATE incidents SET
                                                        zona=:zona, servicio=:servicio, categoria=:categoria, equipo_afectado=:equipo,
                                                        fecha_inicio=:fecha_inicio, hora_inicio=:hora_inicio, fecha_fin=:fecha_fin,
                                                        hora_fin=:hora_fin, clientes_afectados=:clientes, causa_raiz=:causa,
                                                        descripcion=:descripcion, duracion_horas=:duracion, conocimiento_tiempos=:conocimiento
                                                    WHERE id=:id
                                                """), {
                                                    "zona": str(edit_row.get('zona', '')), "servicio": str(edit_row.get('servicio', '')),
                                                    "categoria": str(edit_row.get('categoria', '')), "equipo": str(edit_row.get('equipo_afectado', '')),
                                                    "fecha_inicio": f_i_s, "hora_inicio": sql_hi, "fecha_fin": f_f_s, "hora_fin": sql_hf,
                                                    "clientes": int(edit_row.get('clientes_afectados', 0)), "causa": str(edit_row.get('causa_raiz', '')),
                                                    "descripcion": str(edit_row.get('descripcion', '')), "duracion": float(dur_r),
                                                    "conocimiento": conoce_s, "id": row_id
                                                })
                                    st.success("✅ Modificaciones integradas. Recalculando métricas...")
                                    time.sleep(1)
                                    st.cache_data.clear()
                                    st.rerun()
                                except Exception as e: st.error(f"Error al guardar: {e}")

        # --- ARCHIVO HISTÓRICO MASIVO ---
        st.divider()
        st.header("📂 Histórico Consolidado (Todos los Meses)")
        st.markdown("Despliegue cualquier sección para auditar incidentes pasados de otros meses.")

        meses_con_datos = [m for m in meses_nombres if m in df_total['mes_nombre'].unique()]
        otros_meses = [m for m in meses_con_datos if m != mes_seleccionado]

        if otros_meses:
            for mes in otros_meses:
                with st.expander(f"📁 Consultar Registro Histórico: {mes}"):
                    export_cols_h = [c for c in df_total.columns if c not in ['fecha_convertida', 'mes_nombre', 'id', 'gsheet_id', 'worksheet_name', 'lat', 'lon']]
                    df_hist = df_total[df_total['mes_nombre'] == mes][export_cols_h]
                    st.dataframe(df_hist, use_container_width=True, hide_index=True)
                    csv_h = df_hist.to_csv(index=False).encode('utf-8')
                    st.download_button(label=f"📥 Descargar Resumen Excel ({mes})", data=csv_h, file_name=f"Historico_NOC_{mes}.csv", mime='text/csv', key=f"btn_{mes}")
