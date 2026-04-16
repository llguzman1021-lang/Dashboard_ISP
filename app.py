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
        st.write("⏱️ **Ventana Temporal de Inicio**")
        
        c_i1, c_i2 = st.columns(2)
        conoce_f_i = c_i1.radio("🗓️ ¿Conoce Fecha de Inicio?", ["Sí", "No"], horizontal=True)
        conoce_h_i = c_i2.radio("🕒 ¿Conoce Hora de Inicio?", ["Sí", "No"], horizontal=True)
        
        c1, c2 = st.columns(2)
        f_i = datetime.now().date()
        h_i = datetime.now().time()
        final_f_i = "N/A"
        final_h_i = "N/A"

        if conoce_f_i == "Sí":
            f_i = c1.date_input("🗓️ Fecha de Inicio")
            final_f_i = f_i.strftime("%d/%m/%Y")
        if conoce_h_i == "Sí":
            h_i = c2.time_input("🕒 Hora de Apertura")
            final_h_i = h_i.strftime("%H:%M:%S")
        
        st.write("---")
        st.write("📉 **Estado de Cierre (Cálculo de Tiempos)**")
        
        c_c1, c_c2 = st.columns(2)
        conoce_f_f = c_c1.radio("🗓️ ¿Conoce Fecha de Cierre?", ["Sí", "No"], horizontal=True)
        conoce_h_f = c_c2.radio("🕒 ¿Conoce Hora de Cierre?", ["Sí", "No"], horizontal=True)
        
        st.info("ℹ️ Si selecciona 'No' en fecha u hora, el sistema registrará 'N/A' y la duración como 0h.")
        
        c3, c4 = st.columns(2)
        final_f = "N/A"
        final_h = "N/A"
        duracion = 0
        desc_conocimiento = "Total"

        if conoce_f_f == "Sí" and conoce_h_f == "Sí":
            f_f = c3.date_input("🗓️ Fecha de Cierre")
            h_f = c4.time_input("🕒 Hora de Cierre")
            final_f = f_f.strftime("%d/%m/%Y")
            final_h = h_f.strftime("%H:%M:%S")
            desc_conocimiento = "Total"
            
            try:
                if conoce_f_i == "Sí" and conoce_h_i == "Sí":
                    dt_i = datetime.combine(f_i, h_i)
                    dt_f = datetime.combine(f_f, h_f)
                    duracion = round((dt_f - dt_i).total_seconds() / 3600, 2)
                    if duracion < 0:
                        st.error("Error: La fecha/hora de cierre no puede ser anterior a la de inicio.")
                        duracion = 0
                else:
                    duracion = 0
            except:
                duracion = 0
        
        elif conoce_f_f == "Sí" and conoce_h_f == "No":
            f_f = c3.date_input("🗓️ Fecha de Cierre")
            final_f = f_f.strftime("%d/%m/%Y")
            final_h = "N/A"
            duracion = 0 
            desc_conocimiento = "Parcial (Solo Fecha)"

        elif conoce_f_f == "No" and conoce_h_f == "Sí":
            h_f = c4.time_input("🕒 Hora de Cierre")
            final_f = "N/A"
            final_h = h_f.strftime("%H:%M:%S")
            duracion = 0 
            desc_conocimiento = "Parcial (Solo Hora)"
        
        else:
            final_f = "N/A"
            final_h = "N/A"
            duracion = 0
            desc_conocimiento = "Ninguno"

        st.write("---")
        default_clientes = 1 if categoria == "Cliente Corporativo" else 0
        clientes = st.number_input("👥 Usuarios/Clientes Afectados", min_value=0, step=1, value=default_clientes)
        
        causa = st.selectbox("🔍 Diagnóstico Causa Raíz", [
            "Corte de Fibra Óptica", "Inestabilidad Suministro Eléctrico", 
            "Desajuste de Configuración", "Vandalismo / Sabotaje", 
            "Degradación de Hardware", "Condiciones Atmosféricas Adversas", "Daños por Fauna Sinantrópica"
        ])
        desc = st.text_area("📝 Detalles Técnicos / Descripción")
        
        if st.form_submit_button("Guardar Registro Operativo"):
            nueva_fila = [
                str(zona), str(servicio), str(categoria), str(equipo), str(final_f_i), str(final_h_i), 
                str(final_f), str(final_h), int(clientes), str(causa), str(desc), float(duracion), str(desc_conocimiento)
            ]
            sheet.append_row(nueva_fila)
            st.toast("✅ Base de datos operativa actualizada satisfactoriamente")
            time.sleep(1)
            st.rerun()

# --- PROCESAMIENTO Y ANALÍTICA DE DATOS ---
try:
    records = sheet.get_all_records()
    if records:
        df_total = pd.DataFrame(records)
        df_total.columns = df_total.columns.str.strip() # Limpieza de nombres de columnas
        
        df_total['gsheet_id'] = range(2, len(df_total) + 2)
        df_total['Fecha_Convertida'] = pd.to_datetime(df_total['Fecha_Inicio'], dayfirst=True, errors='coerce')
        df_total['Mes_Nombre'] = df_total['Fecha_Convertida'].dt.month.map(lambda x: meses_nombres[int(x)-1] if pd.notnull(x) else None)

        df_mes = df_total[df_total['Mes_Nombre'] == mes_seleccionado].copy()

        st.title(f"📊 Dashboard Operacional NOC: {mes_seleccionado} {datetime.now().year}")

        if not df_mes.empty:
            k_mttr, k_imp, k_max, k_down = st.columns(4)
            df_mttr = df_mes[df_mes['Duracion_Horas'] > 0]
            avg_mttr = df_mttr['Duracion_Horas'].mean() if not df_mttr.empty else 0
            
            k_mttr.metric("⏱️ MTTR (Promedio)", f"{avg_mttr:.2f} h")
            k_imp.metric("👥 Impacto Acumulado", f"{int(df_mes['Clientes_Afectados'].sum())}")
            k_max.metric("🚨 Máxima Indisponibilidad", f"{df_mes['Duracion_Horas'].max():.2f} h")
            k_down.metric("⏳ Downtime Total", f"{df_mes['Duracion_Horas'].sum():.2f} h")

            st.write("---")
            if 'Servicio' in df_mes.columns:
                df_serv = df_mes.groupby('Servicio').size().reset_index(name='Total_Eventos')
                fig_serv_kpi = px.bar(df_serv, x='Total_Eventos', y='Servicio', orientation='h',
                                    title="📡 <b>Composición por Servicio Afectado</b>",
                                    labels={'Total_Eventos': 'Total de Eventos NOC', 'Servicio': ''},
                                    text_auto=True, template="plotly_dark", color='Servicio', color_discrete_sequence=['#0068c9', '#ff9f43', '#27ae60'])
                fig_serv_kpi.update_layout(showlegend=False, height=250, margin=dict(l=0, r=0, t=60, b=0))
                st.plotly_chart(fig_serv_kpi, use_container_width=True)

            st.divider()
            df_trend = df_mes.groupby('Fecha_Convertida').size().reset_index(name='Total_Eventos')
            fig_trend = px.area(df_trend, x='Fecha_Convertida', y='Total_Eventos', 
                                title="📊 <b>Análisis de Estabilidad de Red (Día a Día)</b>", template="plotly_dark")
            fig_trend.update_traces(line_color='#0068c9', fillcolor='rgba(0, 104, 201, 0.2)')
            st.plotly_chart(fig_trend, use_container_width=True)

            fig_pie = px.pie(df_mes, names='Categoria', title="📂 <b>Composición de Cartera Afectada</b>", 
                            hole=0.6, template="plotly_dark", color_discrete_sequence=['#0068c9', '#ff4b4b'])
            st.plotly_chart(fig_pie, use_container_width=True)
            
            top_zonas = df_mes.groupby('Zona')['Duracion_Horas'].sum().nlargest(5).reset_index()
            top_zonas.columns = ['Zona', 'Horas Offline']
            fig_bar = px.bar(top_zonas, x='Horas Offline', y='Zona', orientation='h',
                             title="📈 <b>Puntos Críticos de Indisponibilidad</b>", template="plotly_dark")
            st.plotly_chart(fig_bar, use_container_width=True)

            # --- BITÁCORA ---
            st.divider()
            st.subheader(f"🔍 Auditoría y Gestión de Bitácora Operativa: {mes_seleccionado}")
            
            df_display = df_mes.copy()
            df_display.insert(0, "Seleccionar", False)
            
            edited_df = st.data_editor(
                df_display.drop(columns=['Fecha_Convertida', 'Mes_Nombre']),
                column_config={"Seleccionar": st.column_config.CheckboxColumn(default=False), "gsheet_id": None},
                use_container_width=True, hide_index=True, key="main_editor"
            )

            if st.button("💾 Sincronizar Ediciones Operativas"):
                for i in range(len(edited_df)):
                    fila = edited_df.iloc[i].copy()
                    row_idx = int(fila['gsheet_id'])
                    
                    # Recálculo de duración para asegurar consistencia
                    try:
                        f_i_s, h_i_s = str(fila['Fecha_Inicio']), str(fila['Hora_Inicio'])
                        f_f_s, h_f_s = str(fila['Fecha_Fin']), str(fila['Hora_Fin'])
                        if "N/A" not in [f_i_s, h_i_s, f_f_s, h_f_s]:
                            dt_ini = datetime.strptime(f"{f_i_s} {h_i_s}", "%d/%m/%Y %H:%M:%S")
                            dt_fin = datetime.strptime(f"{f_f_s} {h_f_s}", "%d/%m/%Y %H:%M:%S")
                            fila['Duracion_Horas'] = round((dt_fin - dt_ini).total_seconds() / 3600, 2)
                    except: pass

                    row_values = [str(x) if not isinstance(x, (int, float)) else x for x in fila.drop(['gsheet_id', 'Seleccionar']).tolist()]
                    sheet.update(f"A{row_idx}:M{row_idx}", [row_values])
                
                st.success("✅ Sincronización completa.")
                time.sleep(1)
                st.rerun()
            
            csv_m = df_mes.drop(columns=['gsheet_id', 'Fecha_Convertida', 'Mes_Nombre']).to_csv(index=False).encode('utf-8')
            st.download_button(label="📥 Exportar Reporte (CSV)", data=csv_m, file_name=f"Reporte_NOC_{mes_seleccionado}.csv", mime='text/csv')

        else:
            st.info(f"No se detectan registros para {mes_seleccionado}.")
    else:
        st.info("La base de datos está vacía.")
except Exception as e:
    st.error(f"Error Crítico: {e}")
