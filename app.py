"""
ClimaCaribe Dashboard - Streamlit Cloud Version
Sistema de Monitoreo Meteorológico en Tiempo Real
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

try:
    import psycopg
except ImportError as e:
    st.error(f"❌ Error importing psycopg: {e}")
    st.error("Please ensure psycopg[binary] is in requirements.txt")
    st.stop()

# =====================================================
# CONFIGURACIÓN DE LA PÁGINA
# =====================================================

st.set_page_config(
    page_title="ClimaCaribe - Monitoreo Meteorológico",
    page_icon="🌴",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =====================================================
# CONFIGURACIÓN DE BASE DE DATOS
# =====================================================

@st.cache_resource
def get_db_config():
    """Obtener configuración de base de datos desde secrets o variables de entorno"""
    try:
        # Intentar usar Streamlit secrets (producción)
        if hasattr(st, 'secrets') and 'postgres' in st.secrets:
            return {
                'host': st.secrets['postgres']['host'],
                'dbname': st.secrets['postgres']['database'],
                'user': st.secrets['postgres']['user'],
                'password': st.secrets['postgres']['password'],
                'port': st.secrets['postgres']['port'],
                'sslmode': 'require',
                'connect_timeout': 10
            }
    except Exception as e:
        st.error(f"Error leyendo secrets: {e}")
    
    # Fallback: variables de entorno (desarrollo local)
    import os
    return {
        'host': os.environ.get('DATABASE_HOST', 'localhost'),
        'database': os.environ.get('DATABASE_NAME', 'postgres'),
        'user': os.environ.get('DATABASE_USER', 'postgres'),
        'password': os.environ.get('DATABASE_PASSWORD', ''),
        'port': os.environ.get('DATABASE_PORT', '5432'),
        'sslmode': 'require',
        'connect_timeout': 10
    }

DB_CONFIG = get_db_config()

# =====================================================
# FUNCIONES DE BASE DE DATOS
# =====================================================

@st.cache_data(ttl=30)
def get_recent_data(hours=24, region_filter=None):
    """Obtener datos recientes de la base de datos"""
    try:
        conn = psycopg.connect(**DB_CONFIG)
        
        base_query = """
        SELECT 
            f.ts,
            l.city,
            l.region,
            f.temperature,
            f.feels_like,
            f.humidity,
            f.pressure,
            f.wind_speed,
            f.uv_index,
            f.status,
            f.data_source
        FROM stream.fact_weather_stream f
        JOIN stream.dim_location l ON f.location_id = l.location_id
        WHERE f.ts >= NOW() - INTERVAL '{} hours'
        """.format(hours)
        
        if region_filter and region_filter != "Todas":
            base_query += f" AND l.region = '{region_filter}'"
        
        base_query += " ORDER BY f.ts DESC"
        
        df = pd.read_sql(base_query, conn)
        conn.close()
        
        return df
    except Exception as e:
        st.error(f"❌ Error conectando a base de datos: {e}")
        st.info("💡 Verifica que los secrets estén configurados correctamente")
        return pd.DataFrame()

@st.cache_data(ttl=30)
def get_active_alerts():
    """Obtener alertas activas"""
    try:
        conn = psycopg.connect(**DB_CONFIG)
        
        query = """
        SELECT 
            a.detected_at,
            l.city,
            l.region,
            a.severity,
            a.metric_name,
            a.metric_value,
            a.threshold_value,
            a.description,
            a.recommendations
        FROM stream.fact_alert a
        JOIN stream.dim_location l ON a.location_id = l.location_id
        WHERE a.detected_at >= NOW() - INTERVAL '24 hours'
        ORDER BY 
            CASE a.severity
                WHEN 'CRITICAL' THEN 1
                WHEN 'WARNING' THEN 2
                WHEN 'CAUTION' THEN 3
                ELSE 4
            END,
            a.detected_at DESC
        LIMIT 50
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        return df
    except Exception as e:
        st.error(f"❌ Error obteniendo alertas: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def get_summary_metrics(hours=24):
    """Obtener métricas de resumen"""
    try:
        conn = psycopg.connect(**DB_CONFIG)
        
        query = f"""
        SELECT 
            COUNT(*) as total_eventos,
            COUNT(DISTINCT location_id) as estaciones_activas,
            AVG(temperature) as temp_promedio,
            MAX(temperature) as temp_maxima,
            MIN(temperature) as temp_minima,
            COUNT(CASE WHEN status != 'normal' THEN 1 END) as alertas_totales
        FROM stream.fact_weather_stream
        WHERE ts >= NOW() - INTERVAL '{hours} hours'
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        return df.iloc[0] if not df.empty else None
    except Exception as e:
        st.error(f"❌ Error obteniendo métricas: {e}")
        return None

# =====================================================
# INTERFAZ DE USUARIO
# =====================================================

# Título principal
st.title("🌴 ClimaCaribe - Monitoreo Meteorológico")
st.markdown("### Sistema de Alertas del Caribe Colombiano | *Últimas 24 horas*")

# Sidebar - Configuración
st.sidebar.header("⚙️ Configuración")

# Rango de tiempo
time_range = st.sidebar.selectbox(
    "📅 Rango de Tiempo",
    options=[6, 12, 24, 48, 72],
    index=2,
    format_func=lambda x: f"Últimas {x} horas"
)

# Filtro por región
regions = ["Todas", "Atlántico", "Bolívar", "Magdalena", "Cesar", "Córdoba", 
           "Cundinamarca", "Antioquia", "Valle del Cauca"]
region_filter = st.sidebar.selectbox(
    "🌍 Filtrar por Región",
    options=regions
)

# Auto-actualización
auto_refresh = st.sidebar.checkbox("🔄 Auto-actualización (30 seg)", value=True)

if auto_refresh:
    st.sidebar.info("⏱️ Próxima actualización en 30 segundos")

# Umbral de anomalías
z_threshold = st.sidebar.slider(
    "🎯 Umbral de Anomalías (z-score)",
    min_value=1.5,
    max_value=4.0,
    value=2.5,
    step=0.5,
    help="Mayor valor = menos sensible a anomalías"
)

# =====================================================
# MÉTRICAS PRINCIPALES
# =====================================================

st.markdown("---")

# Obtener métricas
metrics = get_summary_metrics(time_range)

if metrics is not None:
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric(
            "📊 Total Eventos",
            f"{int(metrics['total_eventos']):,}",
            delta=None
        )
    
    with col2:
        st.metric(
            "🗺️ Estaciones",
            int(metrics['estaciones_activas'])
        )
    
    with col3:
        delta_temp = metrics['temp_promedio'] - 28.0
        st.metric(
            "🌡️ Temp. Promedio",
            f"{metrics['temp_promedio']:.1f}°C",
            delta=f"{delta_temp:+.1f}°C"
        )
    
    with col4:
        status = "Extrema" if metrics['temp_maxima'] > 38 else "Normal"
        st.metric(
            "🔥 Temp. Máxima",
            f"{metrics['temp_maxima']:.1f}°C",
            delta=status,
            delta_color="inverse" if status == "Extrema" else "normal"
        )
    
    with col5:
        st.metric(
            "❄️ Temp. Mínima",
            f"{metrics['temp_minima']:.1f}°C"
        )
    
    with col6:
        st.metric(
            "🚨 Alertas",
            f"{int(metrics['alertas_totales']):,}",
            delta="¡Atención!" if metrics['alertas_totales'] > 100 else None,
            delta_color="inverse" if metrics['alertas_totales'] > 100 else "normal"
        )

# =====================================================
# ALERTAS ACTIVAS
# =====================================================

st.markdown("---")
st.header("🚨 Alertas Meteorológicas Activas")

alerts_df = get_active_alerts()

if not alerts_df.empty:
    # Filtrar por región si aplica
    if region_filter != "Todas":
        alerts_df = alerts_df[alerts_df['region'] == region_filter]
    
    if not alerts_df.empty:
        for idx, alert in alerts_df.head(5).iterrows():
            severity_emoji = {
                'CRITICAL': '🔴',
                'WARNING': '🟠',
                'CAUTION': '🟡',
                'NORMAL': '🟢'
            }
            
            emoji = severity_emoji.get(alert['severity'], '⚪')
            
            with st.expander(f"{emoji} {alert['description']}", expanded=(idx == 0)):
                col_a, col_b = st.columns([2, 1])
                
                with col_a:
                    st.markdown(f"**📍 Ciudad:** {alert['city']}, {alert['region']}")
                    st.markdown(f"**📊 Valor:** {alert['metric_value']:.1f}")
                    st.markdown(f"**🌡️ Descripción:** {alert['description']}")
                    if alert['recommendations']:
                        st.markdown(f"**💡 Recomendaciones:** {alert['recommendations']}")
                
                with col_b:
                    st.markdown(f"**🕐 Detectada:** {alert['detected_at'].strftime('%H:%M:%S')}")
                    st.markdown(f"**⚠️ Severidad:** {alert['severity']}")
    else:
        st.info("✅ No hay alertas activas para la región seleccionada")
else:
    st.info("✅ No hay alertas activas en las últimas 24 horas")

# =====================================================
# DATOS RECIENTES Y VISUALIZACIONES
# =====================================================

st.markdown("---")
st.header("📈 Datos en Tiempo Real")

# Obtener datos
df = get_recent_data(time_range, region_filter if region_filter != "Todas" else None)

if not df.empty:
    # Tabs para diferentes vistas
    tab1, tab2, tab3 = st.tabs(["🌡️ Temperaturas", "💧 Humedad & Presión", "📊 Datos Crudos"])
    
    with tab1:
        # Gráfico de temperaturas
        fig = px.line(
            df,
            x='ts',
            y='temperature',
            color='city',
            title='Evolución de Temperatura por Ciudad',
            labels={'ts': 'Tiempo', 'temperature': 'Temperatura (°C)', 'city': 'Ciudad'}
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_humidity = px.box(
                df,
                x='city',
                y='humidity',
                title='Distribución de Humedad por Ciudad',
                labels={'humidity': 'Humedad (%)', 'city': 'Ciudad'}
            )
            st.plotly_chart(fig_humidity, use_container_width=True)
        
        with col2:
            fig_pressure = px.scatter(
                df,
                x='temperature',
                y='pressure',
                color='city',
                title='Temperatura vs Presión',
                labels={'temperature': 'Temperatura (°C)', 'pressure': 'Presión (hPa)'}
            )
            st.plotly_chart(fig_pressure, use_container_width=True)
    
    with tab3:
        # Mostrar últimos registros
        st.dataframe(
            df.head(100),
            use_container_width=True,
            height=400
        )
        
        # Botón de descarga
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Descargar CSV",
            data=csv,
            file_name=f"climacaribe_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
else:
    st.warning("⚠️ No hay datos disponibles para el rango de tiempo seleccionado")

# =====================================================
# FOOTER
# =====================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray;'>
    <p>🌴 ClimaCaribe Dashboard | Desarrollado con Streamlit | Datos actualizados cada 30 segundos</p>
    <p>Sistema de Monitoreo Meteorológico del Caribe Colombiano</p>
</div>
""", unsafe_allow_html=True)

# Auto-refresh
if auto_refresh:
    import time
    time.sleep(30)
    st.rerun()


