```python
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
from datetime import datetime
import calendar
import time

# ---------------- CONFIGURACIÓN GENERAL ----------------
st.set_page_config(
    page_title="Multinet NOC Analytics | Enterprise Operations",
    layout="wide",
    page_icon="🌐"
)

# ---------------- CONEXIÓN A NEON ----------------
@st.cache_resource
def get_engine():
    return create_engine(
        st.secrets["neon_dsn"],
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args={"connect_timeout": 10}
    )

engine = get_engine()

# ---------------- CARGA DE DATOS ----------------
@st.cache_data(ttl=300)
def load_data():
    query = "SELECT * FROM incidents ORDER BY id ASC"
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    # Normalización de tipos esperados desde Neon (DATE/TIME)
    if "fecha_inicio" in df.columns:
        df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], errors="coerce").dt.strftime("%d/%m/%Y")
    if "fecha_fin" in df.columns:
        df["fecha_fin"] = pd.to_datetime(df["fecha_fin"], errors="coerce").dt.strftime("%d/%m/%Y")

    if "hora_inicio" in df.columns:
        df["hora_inicio"] = df["hora_inicio"].astype(str)
    if "hora_fin" in df.columns:
        df["hora_fin"] = df["hora_fin"].astype(str)

    return df

# ---------------- FUNCIÓN DE MÉTRICAS ----------------
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
            intervalos = sorted(list(zip(s[m].tolist(), e[m].tolist())), key=lambda x: x[0])
            merged = [list(intervalos[0])]
            for current in intervalos[1:]:
                if current[0] <= merged[-1][1]:
                    merged[-1][1] = max(merged[-1][1], current[1])
                else:
                    merged.append(list(current))
            tiempo_real = sum((fin - ini).total_seconds() for ini, fin in merged) / 3600.0

    sla_resultante = max(0.0, min(100.0, ((horas_mes_total - tiempo_real) / horas_mes_total) * 100))
    mttr = df_kpi[df_kpi['duracion_horas'] > 0]['duracion_horas'].mean() if not df_kpi[df_kpi['duracion_horas'] > 0].empty else 0.0
    clientes = int(df_kpi['clientes_afectados'].sum())
    max_downt = df_kpi['duracion_horas'].max() if not df_kpi.empty else 0.0

    return downtime_bruto, acd, sla_resultante, mttr, clientes, max_downt

# ---------------- ESTILOS ----------------
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

# ---------------- SIDEBAR: FORMULARIO ----------------
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
        hora_inicio_final = h_i
    else:
        h_i = None
        hora_inicio_final = None

    st.write("---")
    st.write("✅ **Registro de Tiempos: Resolución y Cierre**")

    c_c1, c_c2 = st.columns(2)
    f_f = c_c1.date_input("📅 Fecha de Cierre")
    conoce_h_f = c_c1.radio("🕒 ¿Conoce la Hora Exacta?", ["No","Sí"], horizontal=True)

    if conoce_h_f == "Sí":
        h_f = c_c2.time_input("🕒 Hora de Cierre")
        hora_fin_final = h_f
    else:
        h_f = None
        hora_fin_final = None

    duracion = 0.0
    if conoce_h_i == "Sí" and conoce_h_f == "Sí":
        desc_conocimiento = "Total"
        try:
            dt_i = datetime.combine(f_i, h_i)
            dt_f = datetime.combine(f_f, h_f)
            duracion = round((dt_f - dt_i).total_seconds() / 3600, 2)
            if duracion < 0:
                duracion = 0.0
        except:
            duracion = 0.0
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
        "Mantenimiento Programado","Vandalismo o Hurto","Condiciones Climáticas",
        "No Especificado"
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
                    "fecha_inicio": f_i,          # DATE en Neon
                    "hora_inicio": hora_inicio_final,  # TIME en Neon (o NULL)
                    "fecha_fin": f_f,
                    "hora_fin": hora_fin_final,
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

# ---------------- PROCESAMIENTO PRINCIPAL ----------------
try:
    df_total = load_data()

    if df_total.empty:
        st.info("No hay datos en la base de datos todavía.")
        st.stop()

    df_total.columns = [c.lower() for c in df_total.columns]

    # Conversión robusta de fecha_inicio a datetime para análisis
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

    df_total['duracion_horas'] = pd.to_numeric(df_total['duracion_horas'], errors='coerce').fillna(0.0)
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

        # --- COMPARATIVA CON MES ANTERIOR ---
        delta_m = None
        delta_a = None
        delta_s = None
        delta_dias = None

        if mes_index > 1:
            mes_pasado_nom = meses_nombres[mes_index - 2]
            dias_pasado = calendar.monthrange(anio_actual, mes_index - 1)[1]
            df_pasado = df_total[df_total['mes_nombre'] == mes_pasado_nom].copy()

            if not df_pasado.empty:
                d_b_p, acd_p, sla_p, mttr_p, _, _ = calcular_metricas(df_pasado, dias_pasado * 24)
                if mttr_p > 0:
                    delta_m = f"{avg_mttr - mttr_p:+.1f} horas"
                if acd_p > 0:
                    delta_a = f"{acd_horas - acd_p:+.1f} horas"
                if sla_p > 0:
                    delta_s = f"{sla_porcentaje - sla_p:+.2f}%"
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

        # --- GRÁFICAS ---
        st.divider()
        st.subheader("📈 Análisis Visual del Rendimiento Operativo")

        col_g1, col_g2 = st.columns(2)

        causas_cortas = {
            "Corte de Fibra por Terceros": "Terceros",
            "Corte de Fibra (No Especificado)": "Fibra",
            "Caída de Árboles sobre Fibra": "Árboles",
            "Falla de Energía Comercial": "Energía",
            "Corrosión en Equipos": "Corrosión",
            "Daños por Fauna": "Fauna",
            "Falla de Hardware": "Hardware",
            "Falla de Configuración": "Configuración",
            "Falla de Redundancia": "Redundancia",
            "Saturación de Tráfico": "Saturación",
            "Saturación en Servidor UNIFI": "Sat. UNIFI",
            "Falla de Inicio en UNIFI": "Inic. UNIFI",
            "Mantenimiento Programado": "Mantenimiento",
            "Vandalismo o Hurto": "Vandalismo",
            "Condiciones Climáticas": "Clima",
            "No Especificado": "N/E"
        }

        df_caus = df_filtrado.groupby('causa_raiz').size().reset_index(name='Alertas')
        df_caus['Causa_Corta'] = df_caus['causa_raiz'].map(lambda x: causas_cortas.get(x, str(x).split()[0]))

        fig_rca = px.pie(df_caus, names='Causa_Corta', values='Alertas', hole=0.5,
                         title="🔍 <b>Causas Principales de las Fallas Registradas</b>", template="plotly_dark")
        fig_rca.update_traces(textposition='inside', textinfo='percent+label',
                              marker=dict(line=dict(color='#000000', width=1)))
        fig_rca.update_layout(showlegend=False, margin=dict(l=0, r=0, t=60, b=0))
        col_g1.plotly_chart(fig_rca, use_container_width=True)

        df_req = df_filtrado.groupby('equipo_afectado').size().reset_index(name='Fallos').sort_values('Fallos', ascending=True)
        fig_eq = px.bar(df_req, x='Fallos', y='equipo_afectado', orientation='h', color='Fallos',
                        title="🛠️ <b>Fallas Acumuladas por Tipo de Equipamiento</b>", template="plotly_dark",
                        color_continuous_scale="Reds")
        fig_eq.update_layout(coloraxis_showscale=False, margin=dict(l=0, r=0, t=60, b=0),
                             xaxis_title="Cantidad de Eventos (Fallas)", yaxis_title="")
        col_g2.plotly_chart(fig_eq, use_container_width=True)

        st.write("")
        col_g3, col_g4 = st.columns(2)

        df_serv = df_filtrado.groupby('servicio').size().reset_index(name='Total_Eventos')
        fig_serv_kpi = px.bar(df_serv, x='Total_Eventos', y='servicio', orientation='h',
                              title="🌐 <b>Volumen de Incidencias según el Servicio</b>",
                              text_auto=True, template="plotly_dark", color='servicio',
                              color_discrete_sequence=['#0068c9', '#ff9f43', '#27ae60'])
        fig_serv_kpi.update_layout(showlegend=False, margin=dict(l=0, r=0, t=60, b=0),
                                   xaxis_title="Total de Caídas Registradas", yaxis_title="")
        fig_serv_kpi.update_traces(textposition='inside', textfont_size=14, marker_line_width=0)
        col_g3.plotly_chart(fig_serv_kpi, use_container_width=True)

        top_zonas = df_filtrado.groupby('zona')['duracion_horas'].sum().nlargest(5).reset_index()
        top_zonas.columns = ['Zona', 'Horas Offline']
        top_zonas['Etiqueta'] = top_zonas['Horas Offline'].apply(lambda x: f"{x:.2f} horas")
        fig_bar_zonas = px.bar(top_zonas, x='Horas Offline', y='Zona', orientation='h',
                               title="📉 <b>Impacto por Zona (Horas sin Servicio)</b>",
                               text='Etiqueta', template="plotly_dark", color='Horas Offline',
                               color_continuous_scale='Blues')
        fig_bar_zonas.update_layout(coloraxis_showscale=False, yaxis={'categoryorder': 'total ascending'},
                                    margin=dict(l=0, r=0, t=60, b=0),
                                    xaxis_title="Total de Horas sin Servicio", yaxis_title="")
        fig_bar_zonas.update_traces(marker_line_width=0, textfont_size=13, textposition='inside')
        col_g4.plotly_chart(fig_bar_zonas, use_container_width=True)

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
                                        'Horas_Down': 'Horas Totales Caídas (Acumulado)'},
                                template="plotly_dark")
            fig_sc.update_layout(margin=dict(l=0, r=0, t=70, b=0), showlegend=True)
            st.plotly_chart(fig_sc, use_container_width=True)

        # Tendencia Diaria
        st.write("")
        df_trend = df_filtrado.groupby('fecha_convertida').size().reset_index(name='Total_Eventos')
        fig_trend = px.area(df_trend, x='fecha_convertida', y='Total_Eventos',
                            title="📅 <b>Tendencia Diaria de Cortes Operativos</b><br>"
                                  "<sup>Muestra la fluctuación y volumen de las incidencias registradas día a día durante este ciclo.</sup>",
                            labels={'fecha_convertida': 'Fechas del Mes', 'Total_Eventos': 'Cantidad Total de Fallas'},
                            template="plotly_dark")
        fig_trend.update_traces(line_color='#0068c9', fillcolor='rgba(0, 104, 201, 0.2)')
        st.plotly_chart(fig_trend, use_container_width=True)

        # --- BITÁCORA INTELIGENTE PROTEGIDA ---
        st.divider()
        st.subheader("🔐 Panel de Auditoría y Mantenimiento de Datos")

        col_a1, col_a2 = st.columns([3, 2])
        busqueda = col_a1.text_input("🔎 Búsqueda de Registros:",
                                     placeholder="Escriba aquí para ubicar detalles técnicos o zonas geográficas...")
        pin_seguridad = col_a2.text_input("🔑 Ingreso de PIN Restringido:", type="password",
                                          placeholder="Credencial de Administrador (Necesario para Editar o Eliminar)")

        acceso_autorizado = (pin_seguridad == "1010")

        df_display = df_filtrado.copy()

        if busqueda:
            mask = df_display.astype(str).apply(lambda x: x.str.contains(busqueda, case=False, na=False)).any(axis=1)
            df_display = df_display[mask]

        df_display.insert(0, "Seleccionar", False)

        conoce_opciones = ["Total", "Parcial (Solo Fechas)", "Parcial (Falta Hora Cierre)",
                           "Parcial (Falta Hora Inicio)", "Parcial (Solo Fecha)", "Parcial (Solo Hora)", "Ninguno"]
        servicio_opciones = ["Internet", "Cable TV (CATV)", "IPTV (Mnet+)"]

        cols_to_drop = [c for c in ['fecha_convertida', 'mes_nombre'] if c in df_display.columns]

        edited_df = st.data_editor(
            df_display.drop(columns=cols_to_drop),
            column_config={
                "Seleccionar": st.column_config.CheckboxColumn("Sel", default=False),
                "id": None,
                "worksheet_name": None,
                "gsheet_id": None,
                "servicio": st.column_config.SelectboxColumn("Servicio", options=servicio_opciones, required=True),
                "fecha_inicio": st.column_config.DateColumn("F. Inicio", format="DD/MM/YYYY"),
                "hora_inicio": st.column_config.TimeColumn("H. Inicio"),
                "fecha_fin": st.column_config.DateColumn("F. Cierre", format="DD/MM/YYYY"),
                "hora_fin": st.column_config.TimeColumn("H. Cierre"),
                "duracion_horas": st.column_config.NumberColumn("Duración (Horas)", disabled=True, format="%.2f"),
                "conocimiento_tiempos": st.column_config.SelectboxColumn(
                    "Nivel de Precisión del Registro", options=conoce_opciones, required=True)
            },
            use_container_width=True, hide_index=True, num_rows="fixed", key="main_editor"
        )

        filas_para_eliminar = edited_df[edited_df["Seleccionar"] == True]
        original_data = df_display.drop(columns=cols_to_drop + ['Seleccionar'])
        edited_data = edited_df.drop(columns=['Seleccionar'])
        hay_cambios = not original_data.reset_index(drop=True).equals(edited_data.reset_index(drop=True))

        if not filas_para_eliminar.empty or hay_cambios:
            if acceso_autorizado:
                if not filas_para_eliminar.empty:
                    if st.button(f"🗑️ Eliminar Definitivamente ({len(filas_para_eliminar)}) Registros Seleccionados"):
                        ids_eliminar = filas_para_eliminar['id'].tolist()
                        try:
                            with engine.begin() as conn:
                                for rid in ids_eliminar:
                                    conn.execute(text("DELETE FROM incidents WHERE id = :id"), {"id": rid})
                            st.success("✅ Los registros han sido destruidos de forma segura.")
                            time.sleep(1)
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al eliminar: {e}")

                if hay_cambios:
                    if st.button("💾 Guardar Modificaciones y Recalcular Métricas Automáticamente"):
                        try:
                            with engine.begin() as conn:
                                for i in range(len(original_data)):
                                    orig_row = original_data.iloc[i]
                                    edit_row = edited_data.iloc[i]
                                    if not orig_row.equals(edit_row):
                                        f_i_v = edit_row.get('fecha_inicio', None)
                                        h_i_v = edit_row.get('hora_inicio', None)
                                        f_f_v = edit_row.get('fecha_fin', None)
                                        h_f_v = edit_row.get('hora_fin', None)
                                        conoce_s = str(edit_row.get('conocimiento_tiempos', ''))

                                        dur_r = 0.0
                                        try:
                                            if conoce_s == "Total" and h_i_v is not None and h_f_v is not None:
                                                dt_ini = datetime.combine(f_i_v, h_i_v)
                                                dt_fin = datetime.combine(f_f_v, h_f_v)
                                                dur_r = round((dt_fin - dt_ini).total_seconds() / 3600, 2)
                                                if dur_r < 0:
                                                    dur_r = 0.0
                                        except:
                                            dur_r = 0.0

                                        row_id = edit_row.get('id') or orig_row.get('id')
                                        conn.execute(text("""
                                            UPDATE incidents SET
                                                zona = :zona,
                                                servicio = :servicio,
                                                categoria = :categoria,
                                                equipo_afectado = :equipo,
                                                fecha_inicio = :fecha_inicio,
                                                hora_inicio = :hora_inicio,
                                                fecha_fin = :fecha_fin,
                                                hora_fin = :hora_fin,
                                                clientes_afectados = :clientes,
                                                causa_raiz = :causa,
                                                descripcion = :descripcion,
                                                duracion_horas = :duracion,
                                                conocimiento_tiempos = :conocimiento
                                            WHERE id = :id
                                        """), {
                                            "zona": edit_row.get('zona', ''),
                                            "servicio": edit_row.get('servicio', ''),
                                            "categoria": edit_row.get('categoria', ''),
                                            "equipo": edit_row.get('equipo_afectado', ''),
                                            "fecha_inicio": f_i_v,
                                            "hora_inicio": h_i_v,
                                            "fecha_fin": f_f_v,
                                            "hora_fin": h_f_v,
                                            "clientes": int(edit_row.get('clientes_afectados', 0)),
                                            "causa": edit_row.get('causa_raiz', ''),
                                            "descripcion": edit_row.get('descripcion', ''),
                                            "duracion": dur_r,
                                            "conocimiento": conoce_s,
                                            "id": row_id
                                        })
                            st.success("✅ Modificaciones integradas exitosamente en la base de datos principal.")
                            time.sleep(1)
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al guardar modificaciones: {e}")
            else:
                st.error("🛑 Operación Restringida: Ingrese el PIN de credencial de administrador para validar estas acciones.")

        # --- Exportación Final ---
        st.write("---")
        export_cols = [c for c in df_filtrado.columns if c not in ['fecha_convertida', 'mes_nombre', 'id', 'gsheet_id', 'worksheet_name']]
        csv_m = df_filtrado[export_cols].to_csv(index=False).encode('utf-8')
        st.download_button("📥 Exportar Análisis del Mes a formato Excel (CSV)", data=csv_m,
                           file_name="Reporte_Directivo_NOC.csv", mime='text/csv')

    else:
        st.info(f"🟢 Excelente estado operativo: No se detectan fallas mayores registradas para el ciclo mensual de {mes_seleccionado}.")

    # --- ARCHIVO HISTÓRICO MASIVO ---
    st.divider()
    st.header("📂 Histórico Consolidado de Datos Operativos (Mensual)")
    st.markdown("A continuación se presenta un desglose de los datos históricos mes a mes registrados en el sistema. Despliegue cualquier sección para auditar incidentes pasados.")

    meses_con_datos = [m for m in meses_nombres if m in df_total['mes_nombre'].unique()]
    otros_meses = [m for m in meses_con_datos if m != mes_seleccionado]

    if otros_meses:
        for mes in otros_meses:
            with st.expander(f"📁 Consultar Registro Histórico Completo: {mes}"):
                export_cols_h = [c for c in df_total.columns if c not in ['fecha_convertida', 'mes_nombre', 'id', 'gsheet_id', 'worksheet_name']]
                df_hist = df_total[df_total['mes_nombre'] == mes][export_cols_h]
                st.dataframe(df_hist, use_container_width=True, hide_index=True)
                csv_h = df_hist.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label=f"📥 Descargar Resumen Excel ({mes})",
                    data=csv_h,
                    file_name=f"Auditoria_Historica_NOC_{mes}.csv",
                    mime='text/csv',
                    key=f"btn_{mes}"
                )

except Exception as e:
    st.error(f"⚠️ Error Interno del Motor de Procesamiento de Datos: {e}")
```
