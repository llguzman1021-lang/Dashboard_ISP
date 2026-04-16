import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
from datetime import datetime, calendar
import time

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Multinet NOC Analytics | Enterprise Operations", layout="wide", page_icon="🌐")

# --- CONEXIÓN SEGURA ---
@st.cache_resource(ttl=300)
def conectar():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds_info = dict(st.secrets["gcp_service_account"])
        creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
        client = gspread.authorize(creds)
        return client.open("Dashboard_ISP").sheet1
    except Exception as e:
        st.error(f"Error de enlace con la base de datos central: {e}")
        return None

sheet = conectar()
if sheet is None: st.stop()

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
    </style>
    """, unsafe_allow_html=True)

# --- SIDEBAR: GESTIÓN OPERATIVA ---
with st.sidebar:
    st.title("🛡️ Centro de Operaciones")
    st.caption("Panel de Control Multinet | v2.5")
    
    meses_nombres = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                     "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    
    mes_actual_num = datetime.now().month
    mes_seleccionado = st.selectbox("📅 Ciclo de Análisis Mensual", meses_nombres, index=mes_actual_num-1)
    
    st.divider()
    st.header("📋 Reporte de Incidencias NOC")
    with st.form("registro_falla", clear_on_submit=True):
        zona = st.text_input("📍 Localización del Nodo/Zona")
        c_serv1, c_serv2 = st.columns([2, 3])
        servicio = c_serv1.selectbox("📡 Servicio Afectado", ["Internet", "Cable TV (CATV)", "IPTV (Mnet+)"])
        categoria = c_serv2.selectbox("👥 Segmentación de Impacto", ["Red Multinet (Troncal)", "Cliente Corporativo"])
        equipo = st.selectbox("⚙️ Equipamiento Afectado", ["OLT", "RB/Mikrotik", "Switch", "ONU", "Servidor", "Fibra Principal", "Caja NAP"])
        
        st.write("---")
        st.write("⏱️ **Apertura**")
        f_i = st.date_input("🗓️ Fecha de Inicio")
        conoce_h_i = st.radio("🕒 ¿Conoce Hora de Apertura?", ["Sí", "No"], horizontal=True, key="h_i_check")
        h_i_final = "N/A"
        if conoce_h_i == "Sí":
            h_i_val = st.time_input("🕒 Hora de Apertura")
            h_i_final = h_i_val.strftime("%H:%M:%S")

        st.write("---")
        st.write("📉 **Cierre**")
        f_f = st.date_input("🗓️ Fecha de Cierre")
        conoce_h_f = st.radio("🕒 ¿Conoce Hora de Cierre?", ["Sí", "No"], horizontal=True, key="h_f_check")
        h_f_final = "N/A"
        if conoce_h_f == "Sí":
            h_f_val = st.time_input("🕒 Hora de Cierre")
            h_f_final = h_f_val.strftime("%H:%M:%S")
        
        duracion = 0
        desc_conocimiento = "Ninguno"
        if conoce_h_i == "Sí" and conoce_h_f == "Sí":
            try:
                dt_i = datetime.combine(f_i, h_i_val)
                dt_f = datetime.combine(f_f, h_f_val)
                duracion = round((dt_f - dt_i).total_seconds() / 3600, 2)
                if duracion < 0: duracion = 0
                desc_conocimiento = "Total"
            except: pass
        elif conoce_h_i == "Sí" or conoce_h_f == "Sí":
            desc_conocimiento = "Parcial"
        
        st.write("---")
        clientes = st.number_input("👥 Usuarios/Clientes Afectados", min_value=0, step=1)
        causa = st.selectbox("🔍 Diagnóstico Causa Raíz", ["Corte de Fibra Óptica", "Inestabilidad Suministro Eléctrico", "Desajuste de Configuración", "Vandalismo / Sabotaje", "Degradación de Hardware", "Condiciones Atmosféricas Adversas", "Daños por Fauna Sinantrópica"])
        desc = st.text_area("📝 Detalles Técnicos")
        
        if st.form_submit_button("Guardar Registro Operativo"):
            nueva_fila = [zona, servicio, categoria, equipo, f_i.strftime("%d/%m/%Y"), h_i_final, f_f.strftime("%d/%m/%Y"), h_f_final, int(clientes), causa, desc, duracion, desc_conocimiento]
            sheet.append_row(nueva_fila)
            st.toast("✅ Registro guardado")
            time.sleep(1)
            st.rerun()

# --- PROCESAMIENTO Y ANALÍTICA ---
try:
    records = sheet.get_all_records()
    if records:
        df_total = pd.DataFrame(records)
        df_total['gsheet_id'] = range(2, len(df_total) + 2)
        df_total['Fecha_Convertida'] = pd.to_datetime(df_total['Fecha_Inicio'], dayfirst=True, errors='coerce')
        df_total['Mes_Nombre'] = df_total['Fecha_Convertida'].dt.month.map(lambda x: meses_nombres[int(x)-1] if pd.notnull(x) else None)
        df_mes = df_total[df_total['Mes_Nombre'] == mes_seleccionado].copy()

        st.title(f"📊 Dashboard Operacional NOC: {mes_seleccionado} {datetime.now().year}")

        if not df_mes.empty:
            # --- KPIs ESTRATÉGICOS ---
            k_mttr, k_imp, k_down, k_sla = st.columns(4)
            df_mttr = df_mes[df_mes['Duracion_Horas'] > 0]
            avg_mttr = df_mttr['Duracion_Horas'].mean() if not df_mttr.empty else 0
            downtime_total = df_mes['Duracion_Horas'].sum()
            
            # Cálculo de SLA%
            num_mes = meses_nombres.index(mes_seleccionado) + 1
            horas_mes = calendar.monthrange(datetime.now().year, num_mes)[1] * 24
            sla_val = max(0, ((horas_mes - downtime_total) / horas_mes) * 100)
            
            k_mttr.metric("⏱️ MTTR (Promedio)", f"{avg_mttr:.2f} horas")
            k_imp.metric("👥 Impacto Acumulado", f"{int(df_mes['Clientes_Afectados'].sum())} clientes")
            k_down.metric("⏳ Downtime Total", f"{downtime_total:.2f} horas")
            k_sla.metric("🎯 Disponibilidad SLA", f"{sla_val:.3f}%", help="Cálculo basado en horas totales del mes vs horas de indisponibilidad.")

            # Gráficas
            st.write("---")
            df_serv = df_mes.groupby('Servicio').size().reset_index(name='Total')
            st.plotly_chart(px.bar(df_serv, x='Total', y='Servicio', orientation='h', title="📡 Composición por Servicio", template="plotly_dark", color='Servicio'), use_container_width=True)

            st.divider()
            df_trend = df_mes.groupby('Fecha_Convertida').size().reset_index(name='Eventos')
            st.plotly_chart(px.area(df_trend, x='Fecha_Convertida', y='Eventos', title="📊 Análisis de Estabilidad (Día a Día)", template="plotly_dark").update_traces(line_color='#0068c9'), use_container_width=True)
            
            # Zonas Críticas con detalle de horas
            top_zonas = df_mes.groupby('Zona')['Duracion_Horas'].sum().nlargest(5).reset_index()
            st.plotly_chart(px.bar(top_zonas, x='Duracion_Horas', y='Zona', orientation='h', title="📈 Puntos Críticos: Cantidad de horas de afectación", labels={'Duracion_Horas': 'Cantidad de horas de afectación'}, template="plotly_dark", color='Duracion_Horas'), use_container_width=True)

            # --- BITÁCORA CON EDICIÓN Y DESCARGA ---
            st.divider()
            st.subheader(f"🔍 Auditoría y Gestión: {mes_seleccionado}")
            df_display = df_mes.copy()
            df_display.insert(0, "Seleccionar", False)
            
            edited_df = st.data_editor(
                df_display.drop(columns=['Fecha_Convertida', 'Mes_Nombre']),
                column_config={
                    "Seleccionar": st.column_config.CheckboxColumn(default=False),
                    "gsheet_id": None,
                    "Duracion_Horas": st.column_config.NumberColumn("Duración (horas)", disabled=True, format="%.2f")
                },
                use_container_width=True, hide_index=True, key="main_editor"
            )

            # Botones de Acción
            c_btn1, c_btn2 = st.columns(2)
            if not edited_df.drop(columns=['Seleccionar']).equals(df_display.drop(columns=['Seleccionar', 'Fecha_Convertida', 'Mes_Nombre'])):
                if c_btn1.button("💾 Sincronizar Cambios"):
                    for i in range(len(edited_df)):
                        fila = edited_df.iloc[i]
                        row_idx = int(fila['gsheet_id'])
                        row_values = [str(x) if not isinstance(x, (int, float)) else x for x in fila.drop(['Seleccionar', 'gsheet_id']).tolist()]
                        sheet.update(f"A{row_idx}:M{row_idx}", [row_values])
                    st.success("Sincronizado")
                    time.sleep(1)
                    st.rerun()

            csv_m = df_mes.drop(columns=['gsheet_id', 'Fecha_Convertida', 'Mes_Nombre']).to_csv(index=False).encode('utf-8')
            c_btn2.download_button(f"📥 Descargar Bitácora {mes_seleccionado}", csv_m, f"Bitacora_{mes_seleccionado}.csv", "text/csv")

        # --- HISTÓRICO ---
        st.divider()
        st.header("📂 Repositorio Histórico")
        meses_con_datos = [m for m in meses_nombres if m in df_total['Mes_Nombre'].unique() and m != mes_seleccionado]
        if meses_con_datos:
            for m in meses_con_datos:
                with st.expander(f"📁 Ciclo: {m}"):
                    df_h = df_total[df_total['Mes_Nombre'] == m].drop(columns=['gsheet_id', 'Fecha_Convertida', 'Mes_Nombre'])
                    st.dataframe(df_h, use_container_width=True, hide_index=True)
                    st.download_button(f"Exportar {m}", df_h.to_csv(index=False).encode('utf-8'), f"NOC_{m}.csv", "text/csv", key=f"dl_{m}")
        else:
            st.info("No hay otros meses con datos.")

    else: st.info("Base de datos vacía.")
except Exception as e: st.error(f"Error: {e}")
