import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
from datetime import datetime
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
    }
    div.stButton > button:first-child:hover {
        background-color: #0056a3;
        color: white;
    }
    [data-testid="stMetricLabel"] { color: #808495 !important; font-size: 15px !important; font-weight: 500 !important; }
    [data-testid="stMetricValue"] { color: #ffffff !important; font-size: 32px !important; font-weight: 700 !important; }
    .stAlert, .stMarkdown { border-radius: 8px; }
    </style>
    """, unsafe_allow_html=True)

# --- SIDEBAR: GESTIÓN OPERATIVA ---
with st.sidebar:
    st.title("🛡️ Centro de Operaciones")
    st.caption("Panel de Control Multinet | v2.6")
    
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
        st.write("⏱️ **Ventana Temporal de Inicio**")
        c1, c2 = st.columns(2)
        f_i = c1.date_input("🗓️ Fecha de Inicio")
        h_i = c2.time_input("🕒 Hora de Apertura")
        
        st.write("---")
        st.write("📉 **Estado de Cierre (Cálculo de Tiempos)**")
        
        c_c1, c_c2 = st.columns(2)
        conoce_f_f = c_c1.radio("🗓️ ¿Conoce Fecha?", ["Sí", "No"], horizontal=True)
        conoce_h_f = c_c2.radio("🕒 ¿Conoce Hora?", ["Sí", "No"], horizontal=True)
        
        c3, c4 = st.columns(2)
        final_f, final_h, duracion, desc_conocimiento = "N/A", "N/A", 0, "Total"

        if conoce_f_f == "Sí" and conoce_h_f == "Sí":
            f_f = c3.date_input("🗓️ Fecha de Cierre")
            h_f = c4.time_input("🕒 Hora de Cierre")
            final_f, final_h = f_f.strftime("%d/%m/%Y"), h_f.strftime("%H:%M:%S")
            try:
                dt_i, dt_f = datetime.combine(f_i, h_i), datetime.combine(f_f, h_f)
                duracion = max(0, round((dt_f - dt_i).total_seconds() / 3600, 2))
            except: duracion = 0
        elif conoce_f_f == "Sí":
            f_f = c3.date_input("🗓️ Fecha de Cierre")
            final_f, desc_conocimiento = f_f.strftime("%d/%m/%Y"), "Parcial (Solo Fecha)"
        elif conoce_h_f == "Sí":
            h_f = c4.time_input("🕒 Hora de Cierre")
            final_h, desc_conocimiento = h_f.strftime("%H:%M:%S"), "Parcial (Solo Hora)"
        else: desc_conocimiento = "Ninguno"

        st.write("---")
        clientes = st.number_input("👥 Usuarios Afectados", min_value=0, step=1)
        causa = st.selectbox("🔍 Diagnóstico Causa Raíz", ["Corte de Fibra Óptica", "Falla Eléctrica", "Configuración", "Vandalismo", "Hardware", "Clima"])
        desc = st.text_area("📝 Detalles Técnicos")
        
        if st.form_submit_button("Guardar Registro Operativo"):
            nueva_fila = [zona, servicio, categoria, equipo, f_i.strftime("%d/%m/%Y"), h_i.strftime("%H:%M:%S"), 
                          final_f, final_h, int(clientes), causa, desc, float(duracion), desc_conocimiento]
            sheet.append_row(nueva_fila)
            st.toast("✅ Registro guardado en base de datos")
            time.sleep(1)
            st.rerun()

# --- ANALÍTICA DE DATOS ---
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
            # --- KPIs ---
            k1, k2, k3, k4 = st.columns(4)
            avg_mttr = df_mes[df_mes['Duracion_Horas'] > 0]['Duracion_Horas'].mean() if not df_mes[df_mes['Duracion_Horas'] > 0].empty else 0
            k1.metric("⏱️ MTTR Promedio", f"{avg_mttr:.2f} h")
            k2.metric("👥 Impacto Total", f"{int(df_mes['Clientes_Afectados'].sum())}")
            k3.metric("🚨 Máx. Caída", f"{df_mes['Duracion_Horas'].max():.2f} h")
            k4.metric("⏳ Downtime Mensual", f"{df_mes['Duracion_Horas'].sum():.2f} h")

            # --- GRÁFICAS APILADAS (UNA SOBRE OTRA) ---
            st.divider()
            
            # Gráfica 1: Composición de Cartera (Donut)
            fig_pie = px.pie(df_mes, names='Categoria', title="📂 <b>Composición de Cartera Afectada</b>", 
                            hole=0.6, template="plotly_dark", color_discrete_sequence=['#0068c9', '#ff4b4b'])
            fig_pie.update_layout(height=400, margin=dict(t=80, b=20))
            st.plotly_chart(fig_pie, use_container_width=True)

            # Gráfica 2: Puntos Críticos (Barras)
            top_zonas = df_mes.groupby('Zona')['Duracion_Horas'].sum().nlargest(5).reset_index()
            fig_bar = px.bar(top_zonas, x='Duracion_Horas', y='Zona', orientation='h',
                             title="📈 <b>Análisis de Indisponibilidad por Zona</b>",
                             labels={'Duracion_Horas': 'Total Horas Offline'},
                             text_auto='.2f', template="plotly_dark", color='Duracion_Horas', color_continuous_scale='Blues')
            fig_bar.update_layout(height=400, coloraxis_showscale=False, yaxis={'categoryorder':'total ascending'}, margin=dict(t=80, b=20))
            st.plotly_chart(fig_bar, use_container_width=True)

            # --- BITÁCORA EDITABLE ---
            st.divider()
            st.subheader(f"🔍 Gestión de Registros: {mes_seleccionado}")
            df_display = df_mes.copy()
            df_display.insert(0, "Seleccionar", False)
            
            edited_df = st.data_editor(
                df_display.drop(columns=['Fecha_Convertida', 'Mes_Nombre']),
                column_config={
                    "Seleccionar": st.column_config.CheckboxColumn(default=False), 
                    "gsheet_id": None,
                    "Servicio": st.column_config.SelectboxColumn("Servicio", options=["Internet", "Cable TV (CATV)", "IPTV (Mnet+)"]),
                    "Conocimiento_Tiempos": st.column_config.SelectboxColumn("Conocimiento", options=["Total", "Parcial (Solo Fecha)", "Parcial (Solo Hora)", "Ninguno"])
                },
                use_container_width=True, hide_index=True, key="editor_noc"
            )

            # BOTÓN ELIMINAR
            if edited_df["Seleccionar"].any():
                if st.button("🗑️ Eliminar Registros Seleccionados"):
                    indices = sorted(edited_df[edited_df["Seleccionar"] == True]['gsheet_id'].tolist(), reverse=True)
                    for idx in indices: sheet.delete_rows(idx)
                    st.rerun()

            # BOTÓN GUARDAR EDICIONES (CON FIX PARA EL ERROR JSON/INT64)
            if st.button("Guardar Cambios Editados"):
                for i in range(len(df_display)):
                    # Comparamos solo las celdas de datos, no el checkbox
                    if not df_display.drop(columns=['Seleccionar','Fecha_Convertida','Mes_Nombre']).iloc[i].equals(edited_df.drop(columns=['Seleccionar']).iloc[i]):
                        fila = edited_df.drop(columns=['Seleccionar']).iloc[i].copy()
                        row_idx = int(fila['gsheet_id'])
                        
                        # Recálculo de duración para la edición
                        try:
                            if fila['Conocimiento_Tiempos'] == "Total" and fila['Fecha_Fin'] != "N/A":
                                d_i = datetime.strptime(f"{fila['Fecha_Inicio']} {fila['Hora_Inicio']}", "%d/%m/%Y %H:%M:%S")
                                d_f = datetime.strptime(f"{fila['Fecha_Fin']} {fila['Hora_Fin']}", "%d/%m/%Y %H:%M:%S")
                                fila['Duracion_Horas'] = max(0, round((d_f - d_i).total_seconds() / 3600, 2))
                            else: fila['Duracion_Horas'] = 0
                        except: pass

                        # --- FIX CRÍTICO: Convertir todo a tipos compatibles con JSON/Gspread ---
                        # Esto elimina el error "int64 is not JSON serializable"
                        lista_valores = []
                        for valor in fila.drop('gsheet_id').tolist():
                            if pd.isna(valor): lista_valores.append("")
                            elif isinstance(valor, (int, float, pd.Timestamp)): lista_valores.append(float(valor) if isinstance(valor, float) else str(valor))
                            else: lista_valores.append(str(valor))
                        
                        sheet.update(f"A{row_idx}:M{row_idx}", [lista_valores])
                st.success("✅ Cambios sincronizados correctamente")
                time.sleep(1)
                st.rerun()

    else:
        st.info("Base de datos vacía.")
except Exception as e:
    st.error(f"Error al procesar datos: {e}")
