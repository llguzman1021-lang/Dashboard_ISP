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
        st.write("⏱️ **Ventana Temporal de Apertura**")
        f_i = st.date_input("🗓️ Fecha de Inicio")
        conoce_h_i = st.radio("🕒 ¿Conoce Hora de Apertura?", ["Sí", "No"], horizontal=True, key="h_i_check")
        
        h_i_final = "N/A"
        if conoce_h_i == "Sí":
            h_i_val = st.time_input("🕒 Hora de Apertura")
            h_i_final = h_i_val.strftime("%H:%M:%S")

        st.write("---")
        st.write("📉 **Estado de Cierre**")
        f_f = st.date_input("🗓️ Fecha de Cierre")
        conoce_h_f = st.radio("🕒 ¿Conoce Hora de Cierre?", ["Sí", "No"], horizontal=True, key="h_f_check")
        
        h_f_final = "N/A"
        if conoce_h_f == "Sí":
            h_f_val = st.time_input("🕒 Hora de Cierre")
            h_f_final = h_f_val.strftime("%H:%M:%S")
        
        # Lógica de cálculo y descripción de conocimiento
        duracion = 0
        desc_conocimiento = "Ninguno"
        
        if conoce_h_i == "Sí" and conoce_h_f == "Sí":
            try:
                dt_i = datetime.combine(f_i, h_i_val)
                dt_f = datetime.combine(f_f, h_f_val)
                duracion = round((dt_f - dt_i).total_seconds() / 3600, 2)
                if duracion < 0:
                    st.error("Error: El cierre no puede ser anterior al inicio.")
                    duracion = 0
                desc_conocimiento = "Total"
            except: pass
        elif conoce_h_i == "Sí" or conoce_h_f == "Sí":
            desc_conocimiento = "Parcial (Solo una hora)"
        
        st.write("---")
        clientes = st.number_input("👥 Usuarios/Clientes Afectados", min_value=0, step=1)
        causa = st.selectbox("🔍 Diagnóstico Causa Raíz", [
            "Corte de Fibra Óptica", "Inestabilidad Suministro Eléctrico", 
            "Desajuste de Configuración", "Vandalismo / Sabotaje", 
            "Degradación de Hardware", "Condiciones Atmosféricas Adversas",
            "Daños por Fauna Sinantrópica"
        ])
        desc = st.text_area("📝 Detalles Técnicos / Descripción")
        
        if st.form_submit_button("Guardar Registro Operativo"):
            nueva_fila = [
                zona, servicio, categoria, equipo, f_i.strftime("%d/%m/%Y"), h_i_final, 
                f_f.strftime("%d/%m/%Y"), h_f_final, int(clientes), causa, desc, duracion, desc_conocimiento
            ]
            sheet.append_row(nueva_fila)
            st.toast("✅ Base de datos operativa actualizada")
            time.sleep(1)
            st.rerun()

# --- PROCESAMIENTO Y ANALÍTICA DE DATOS ---
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
            # --- KPIs ESTRATÉGICOS ACTUALIZADOS ---
            k_mttr, k_imp, k_max, k_down = st.columns(4)
            df_mttr = df_mes[df_mes['Duracion_Horas'] > 0]
            avg_mttr = df_mttr['Duracion_Horas'].mean() if not df_mttr.empty else 0
            
            k_mttr.metric("⏱️ MTTR (Promedio)", f"{avg_mttr:.2f} horas")
            k_imp.metric("👥 Impacto Acumulado", f"{int(df_mes['Clientes_Afectados'].sum())} clientes")
            k_max.metric("🚨 Máxima Indisponibilidad", f"{df_mes['Duracion_Horas'].max():.2f} horas")
            k_down.metric("⏳ Downtime Total", f"{df_mes['Duracion_Horas'].sum():.2f} horas")

            # Gráfica de Servicio
            st.write("---")
            df_serv = df_mes.groupby('Servicio').size().reset_index(name='Total_Eventos')
            fig_serv_kpi = px.bar(df_serv, x='Total_Eventos', y='Servicio', orientation='h',
                                title="📡 <b>Composición por Servicio Afectado</b>",
                                labels={'Total_Eventos': 'Total de Eventos NOC', 'Servicio': ''},
                                text_auto=True, template="plotly_dark", color='Servicio', color_discrete_sequence=['#0068c9', '#ff9f43', '#27ae60'])
            st.plotly_chart(fig_serv_kpi, use_container_width=True)

            # Tendencia Temporal
            st.divider()
            df_trend = df_mes.groupby('Fecha_Convertida').size().reset_index(name='Total_Eventos')
            fig_trend = px.area(df_trend, x='Fecha_Convertida', y='Total_Eventos', 
                                title="📊 <b>Análisis de Estabilidad de Red (Día a Día)</b>",
                                template="plotly_dark")
            fig_trend.update_traces(line_color='#0068c9', fillcolor='rgba(0, 104, 201, 0.2)')
            st.plotly_chart(fig_trend, use_container_width=True)

            # Composición Cartera
            fig_pie = px.pie(df_mes, names='Categoria', title="📂 <b>Composición de Cartera Afectada</b>", 
                            hole=0.6, template="plotly_dark", color_discrete_sequence=['#0068c9', '#ff4b4b'])
            st.plotly_chart(fig_pie, use_container_width=True)
            
            # 3. Top Zonas Críticas (ACTUALIZADO DETALLE DE HORAS)
            top_zonas = df_mes.groupby('Zona')['Duracion_Horas'].sum().nlargest(5).reset_index()
            top_zonas.columns = ['Zona', 'Horas Offline']
            fig_bar = px.bar(top_zonas, x='Horas Offline', y='Zona', orientation='h',
                             title="📈 <b>Puntos Críticos de Indisponibilidad</b><br><sup>Cantidad de horas de afectación acumulada por cada zona</sup>",
                             labels={'Horas Offline': 'Cantidad de horas de afectación'},
                             text_auto='.2f', template="plotly_dark", color='Horas Offline', color_continuous_scale='Blues')
            fig_bar.update_layout(coloraxis_showscale=False, yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_bar, use_container_width=True)

            # --- BITÁCORA ---
            st.divider()
            st.subheader(f"🔍 Gestión de Bitácora: {mes_seleccionado}")
            df_display = df_mes.copy()
            df_display.insert(0, "Seleccionar", False)
            
            conoce_opciones = ["Total", "Parcial (Solo una hora)", "Ninguno"]
            
            edited_df = st.data_editor(
                df_display.drop(columns=['Fecha_Convertida', 'Mes_Nombre']),
                column_config={
                    "Seleccionar": st.column_config.CheckboxColumn(default=False), 
                    "gsheet_id": None,
                    "Duracion_Horas": st.column_config.NumberColumn("Duración (horas)", disabled=True, format="%.2f"),
                    "Conocimiento_Tiempos": st.column_config.SelectboxColumn("Tipo Conocimiento", options=conoce_opciones)
                },
                use_container_width=True, hide_index=True, key="main_editor"
            )

            # Sincronización
            original_data = df_display.drop(columns=['Fecha_Convertida', 'Mes_Nombre', 'Seleccionar'])
            edited_data = edited_df.drop(columns=['Seleccionar'])
            
            if not original_data.equals(edited_data):
                if st.button("💾 Sincronizar Ediciones Operativas"):
                    for i in range(len(original_data)):
                        if not original_data.iloc[i].equals(edited_data.iloc[i]):
                            fila = edited_data.iloc[i].copy()
                            row_idx = int(fila['gsheet_id'])
                            
                            # Recálculo inteligente
                            try:
                                if str(fila['Hora_Inicio']) != "N/A" and str(fila['Hora_Fin']) != "N/A":
                                    dt_ini = datetime.strptime(f"{fila['Fecha_Inicio']} {fila['Hora_Inicio']}", "%d/%m/%Y %H:%M:%S")
                                    dt_fin = datetime.strptime(f"{fila['Fecha_Fin']} {fila['Hora_Fin']}", "%d/%m/%Y %H:%M:%S")
                                    fila['Duracion_Horas'] = round((dt_fin - dt_ini).total_seconds() / 3600, 2)
                                else:
                                    fila['Duracion_Horas'] = 0
                            except: fila['Duracion_Horas'] = 0

                            row_values = [str(x) if not isinstance(x, (int, float)) else x for x in fila.drop('gsheet_id').tolist()]
                            sheet.update(f"A{row_idx}:M{row_idx}", [row_values])
                    
                    st.success("✅ Sincronización completa.")
                    time.sleep(1)
                    st.rerun()

            # Lógica Eliminar
            filas_eliminar = edited_df[edited_df["Seleccionar"] == True]
            if not filas_eliminar.empty:
                if st.button(f"🗑️ Confirmar Borrado ({len(filas_eliminar)})"):
                    indices = sorted(filas_eliminar['gsheet_id'].tolist(), reverse=True)
                    for idx in indices: sheet.delete_rows(idx)
                    st.success("✅ Registros eliminados.")
                    time.sleep(1)
                    st.rerun()

        else:
            st.info(f"Sin registros para {mes_seleccionado}.")

    else:
        st.info("Base de datos vacía.")
except Exception as e:
    st.error(f"Error en el procesamiento: {e}")
