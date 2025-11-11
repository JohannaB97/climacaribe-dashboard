"""
ClimaCaribe Dashboard
Sistema de Monitoreo Meteorológico del Caribe Colombiano en Tiempo Real
"""

import streamlit as st
import psycopg
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import os
import numpy as np

# ============================================
# CONFIGURACIÓN DE PÁGINA
# ============================================

st.set_page_config(
    page_title="ClimaCaribe - Monitoreo en Tiempo Real",
    page_icon="🌴",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================
# CONFIGURACIÓN DE BASE DE DATOS (solo variables de entorno)
# ============================================

DB_CONFIG = {
    'host': os.environ['DB_HOST'],
    'dbname': os.environ['DB_NAME'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'port': os.environ.get('DB_PORT', '5432'),
    'sslmode': 'require'
}

# ============================================
# FUNCIÓN DE CONEXIÓN
# ============================================

@st.cache_resource
def get_db_connection():
    """Crear y cachear conexión a la base de datos PostgreSQL."""
    try:
        conn = psycopg.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        st.error(f"❌ Error conectando a la base de datos: {e}")
        st.info("💡 Verifica tus credenciales en los Secrets de Streamlit Cloud")
        return None


def fetch_data(query, params=None):
    """Ejecutar query y retornar DataFrame (sin cache para datos en tiempo real)."""
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                df = pd.read_sql_query(query, conn, params=params)
            return df
        except Exception as e:
            st.error(f"❌ Error ejecutando consulta: {e}")
            return pd.DataFrame()
    else:
        st.warning("⚠️ Sin conexión a la base de datos.")
        return pd.DataFrame()

# ============================================
# TEST DE CONEXIÓN (puedes dejarlo o comentarlo luego)
# ============================================

#conn_test = get_db_connection()
#if conn_test:
    #st.success("✅ Conexión exitosa a PostgreSQL en Azure")
#else:
    #st.error("❌ No se pudo conectar a la base de datos. Revisa los Secrets.")

# ============================================
# FUNCIONES DE ANÁLISIS
# ============================================

def detect_anomalies(df, column='temperature', threshold=2.5):
    """Detección de anomalías usando z-score"""
    if len(df) > 0 and column in df.columns:
        mean = df[column].mean()
        std = df[column].std()
        if std > 0:
            df['z_score'] = (df[column] - mean) / std
            df['is_anomaly'] = df['z_score'].abs() > threshold
        else:
            df['z_score'] = 0
            df['is_anomaly'] = False
        return df
    return df

def get_color_for_temp(temp):
    """Determinar color según temperatura"""
    if temp >= 35:
        return '#FF0000'  # Rojo - Calor extremo
    elif temp >= 30:
        return '#FF6B00'  # Naranja
    elif temp >= 25:
        return '#FFA500'  # Amarillo-naranja
    elif temp >= 20:
        return '#FFD700'  # Amarillo
    elif temp >= 15:
        return '#90EE90'  # Verde claro
    else:
        return '#4169E1'  # Azul

def format_recommendation(alert_type):
    """Obtener recomendaciones según tipo de alerta"""
    recommendations = {
        'extreme_heat': '🔥 Evitar exposición al sol | Hidratación constante | Buscar lugares frescos',
        'high_heat': '⚠️ Reducir actividad física | Usar protector solar | Mantenerse hidratado',
        'heat_index_critical': '🌡️ NO realizar actividades al aire libre | Permanecer en interiores',
        'heavy_rain': '🌧️ Reducir velocidad al conducir | Evitar zonas de inundación',
        'strong_wind': '💨 Asegurar objetos sueltos | Precaución al conducir',
        'low_pressure': '📉 Mantenerse informado | Posible tormenta en camino'
    }
    return recommendations.get(alert_type, '⚠️ Condiciones anormales - Mantenerse informado')

# ============================================
# CONFIGURACIÓN DE SIDEBAR
# ============================================

st.sidebar.title("⚙️ Configuración")
st.sidebar.markdown("---")

# Selector de rango de tiempo
time_range = st.sidebar.selectbox(
    "📅 Rango de Tiempo",
    [
        "Últimos 5 minutos",
        "Últimos 15 minutos",
        "Últimos 30 minutos",
        "Última 1 hora",
        "Últimas 3 horas",
        "Últimas 6 horas",
        "Últimas 24 horas"
    ],
    index=3  # Default: Última 1 hora
)

# Mapeo de tiempo
time_map = {
    "Últimos 5 minutos": 5,
    "Últimos 15 minutos": 15,
    "Últimos 30 minutos": 30,
    "Última 1 hora": 60,
    "Últimas 3 horas": 180,
    "Últimas 6 horas": 360,
    "Últimas 24 horas": 1440
}

minutes = time_map[time_range]

# Auto-refresh
auto_refresh = st.sidebar.checkbox("🔄 Auto-actualización (30 seg)", value=True)

# Filtro de región
st.sidebar.markdown("---")
region_filter = st.sidebar.multiselect(
    "🌍 Filtrar por Región",
    ["Todas", "Caribe", "Interior"],
    default=["Todas"]
)

# Threshold para outliers
st.sidebar.markdown("---")
anomaly_threshold = st.sidebar.slider(
    "🎯 Umbral de Anomalías (z-score)",
    min_value=1.5,
    max_value=4.0,
    value=2.5,
    step=0.5,
    help="Mayor valor = menos sensible a outliers"
)

# Información del sistema
st.sidebar.markdown("---")
st.sidebar.info("""
💡 **ClimaCaribe v1.0**

Monitoreo en tiempo real de:
- 5 ciudades del Caribe
- 3 ciudades del interior
- 120+ lecturas/minuto
""")

# ============================================
# HEADER PRINCIPAL
# ============================================

st.title("🌴 ClimaCaribe - Monitoreo Meteorológico")
st.markdown(f"**Sistema de Alertas del Caribe Colombiano** | *{time_range}*")

# ============================================
# CONTENEDOR PARA AUTO-REFRESH
# ============================================

placeholder = st.empty()

# Loop principal
iteration = 0
while True:
    iteration += 1
    
    with placeholder.container():
        
        # ==========================================
        # SECCIÓN 1: MÉTRICAS PRINCIPALES (KPIs)
        # ==========================================
        
        query_kpis = f"""
        SELECT 
            COUNT(*) as total_eventos,
            COUNT(DISTINCT station_id) as estaciones_activas,
            COUNT(DISTINCT location_id) as ubicaciones,
            COALESCE(ROUND(AVG(temperature)::numeric, 1), 0) as temp_promedio,
            COALESCE(ROUND(MAX(temperature)::numeric, 1), 0) as temp_maxima,
            COALESCE(ROUND(MIN(temperature)::numeric, 1), 0) as temp_minima,
            COUNT(CASE WHEN status IN ('alert', 'warning', 'critical') THEN 1 END) as alertas_activas,
            MAX(ts) as ultima_actualizacion
        FROM stream.fact_weather_stream
        WHERE ts > (NOW() AT TIME ZONE 'UTC' - INTERVAL '{minutes} minutes')
        """
        
        df_kpis = fetch_data(query_kpis)
        
        if not df_kpis.empty:
            kpi = df_kpis.iloc[0]
            
            col1, col2, col3, col4, col5, col6 = st.columns(6)
            
            with col1:
                st.metric(
                    "📊 Total Eventos",
                    f"{int(kpi['total_eventos']):,}"
                )
            
            with col2:
                st.metric(
                    "📡 Estaciones",
                    int(kpi['estaciones_activas'])
                )
            
            with col3:
                
                if kpi['temp_promedio'] is not None:
                    delta_temp = kpi['temp_promedio'] - 28  # 28°C referencia Caribe
                    st.metric(
                        "🌡️ Temp. Promedio",
                        f"{kpi['temp_promedio']}°C",
                        delta=f"{delta_temp:+.1f}°C",
                        delta_color="inverse" if delta_temp > 5 else "normal"
                )
                else:
                    st.metric(
                        "🌡️ Temp. Promedio",
                        "Sin datos",
                        delta="N/A",
                        delta_color="off"
                )
                            
            with col4:
                st.metric(
                    "🔥 Temp. Máxima",
                    f"{kpi['temp_maxima']}°C",
                    delta="Extrema" if kpi['temp_maxima'] >= 38 else "",
                    delta_color="inverse" if kpi['temp_maxima'] >= 38 else "off"
                )
            
            with col5:
                st.metric(
                    "❄️ Temp. Mínima",
                    f"{kpi['temp_minima']}°C"
                )
            
            with col6:
                alertas = int(kpi['alertas_activas'])
                st.metric(
                    "⚠️ Alertas",
                    alertas,
                    delta="¡Atención!" if alertas > 0 else "Normal",
                    delta_color="inverse" if alertas > 0 else "normal"
                )
        
        st.markdown("---")
        
        # ==========================================
        # SECCIÓN 2: ALERTAS ACTIVAS
        # ==========================================
        
        if not df_kpis.empty and int(df_kpis.iloc[0]['alertas_activas']) > 0:
            st.subheader("🚨 Alertas Meteorológicas Activas")
            
            query_alerts = f"""
            SELECT 
                wa.detected_at,
                dl.city,
                dl.region,
                wa.severity,
                wa.alert_type,
                wa.title,
                wa.description,
                wa.metric_value
            FROM stream.weather_alerts wa
            JOIN stream.dim_location dl ON wa.location_id = dl.location_id
            WHERE wa.status = 'active'
            AND wa.detected_at > NOW() AT TIME ZONE 'UTC' - INTERVAL '{minutes} minutes'
            ORDER BY 
                CASE wa.severity
                    WHEN 'critical' THEN 1
                    WHEN 'high' THEN 2
                    WHEN 'medium' THEN 3
                    ELSE 4
                END,
                wa.detected_at DESC
            LIMIT 10
            """
            
            df_alerts = fetch_data(query_alerts)
            
            if not df_alerts.empty:
                for idx, alert in df_alerts.iterrows():
                    severity_emoji = {
                        'critical': '🔴',
                        'high': '🟠',
                        'medium': '🟡',
                        'low': '🟢'
                    }.get(alert['severity'], '⚪')
                    
                    with st.expander(f"{severity_emoji} {alert['title']}", expanded=(idx < 2)):
                        col1, col2 = st.columns([2, 1])
                        with col1:
                            st.markdown(f"**📍 Ciudad:** {alert['city']}, {alert['region']}")
                            st.markdown(f"**📊 Valor:** {alert['metric_value']}")
                            st.markdown(f"**📝 Descripción:** {alert['description']}")
                            st.markdown(f"**💡 Recomendaciones:**")
                            st.info(format_recommendation(alert['alert_type']))
                        with col2:
                            st.markdown(f"**⏰ Detectada:**")
                            st.write(alert['detected_at'].strftime('%H:%M:%S'))
                            st.markdown(f"**🎚️ Severidad:**")
                            st.write(alert['severity'].upper())
            
            st.markdown("---")
        
        # ==========================================
        # SECCIÓN 3: MAPA INTERACTIVO DE COLOMBIA
        # ==========================================
        
        st.subheader("🗺️ Mapa de Temperaturas en Tiempo Real")
        
        query_map = f"""
        SELECT 
            dl.city,
            dl.region,
            dl.latitude,
            dl.longitude,
            ROUND(AVG(fws.temperature)::numeric, 1) as temp_promedio,
            ROUND(AVG(fws.feels_like)::numeric, 1) as sensacion_termica,
            ROUND(AVG(fws.humidity)::numeric, 1) as humedad_promedio,
            ROUND(AVG(fws.wind_speed)::numeric, 1) as viento_promedio,
            COUNT(CASE WHEN fws.status IN ('alert', 'warning', 'critical') THEN 1 END) as num_alertas,
            MAX(fws.ts) as ultima_lectura
        FROM stream.fact_weather_stream fws
        JOIN stream.dim_location dl ON fws.location_id = dl.location_id
        WHERE fws.ts > NOW() AT TIME ZONE 'UTC' - INTERVAL '{minutes} minutes'
        GROUP BY dl.city, dl.region, dl.latitude, dl.longitude
        """
        
        df_map = fetch_data(query_map)
        
        if not df_map.empty:
            # Filtrar por región si es necesario
            if "Todas" not in region_filter:
                if "Caribe" in region_filter and "Interior" in region_filter:
                    pass  # Mostrar todas
                elif "Caribe" in region_filter:
                    df_map = df_map[df_map['region'].isin(['Atlántico', 'Bolívar', 'Magdalena', 'Cesar', 'Córdoba'])]
                elif "Interior" in region_filter:
                    df_map = df_map[~df_map['region'].isin(['Atlántico', 'Bolívar', 'Magdalena', 'Cesar', 'Córdoba'])]
            
            fig_map = px.scatter_mapbox(
                df_map,
                lat="latitude",
                lon="longitude",
                color="temp_promedio",
                size="temp_promedio",
                hover_name="city",
                hover_data={
                    "temp_promedio": ":.1f°C",
                    "sensacion_termica": ":.1f°C",
                    "humedad_promedio": ":.1f%",
                    "viento_promedio": ":.1f km/h",
                    "num_alertas": True,
                    "latitude": False,
                    "longitude": False
                },
                color_continuous_scale="RdYlBu_r",
                range_color=[8, 38],  # Rango de Colombia
                zoom=5,
                center={"lat": 8.0, "lon": -74.0},  # Centro de Colombia
                height=500,
                labels={
                    "temp_promedio": "Temperatura (°C)",
                    "sensacion_termica": "Sensación térmica",
                    "humedad_promedio": "Humedad",
                    "viento_promedio": "Viento",
                    "num_alertas": "Alertas"
                }
            )
            fig_map.update_layout(
                mapbox_style="open-street-map",
                margin={"r":0,"t":0,"l":0,"b":0}
            )
            st.plotly_chart(fig_map, use_container_width=True, key=f"map_chart_{iteration}")
            
            # Mostrar tabla de ciudades
            st.markdown("#### 📊 Resumen por Ciudad")
            df_map_display = df_map[['city', 'region', 'temp_promedio', 'sensacion_termica', 
                                      'humedad_promedio', 'viento_promedio', 'num_alertas']].copy()
            df_map_display = df_map_display.sort_values('temp_promedio', ascending=False)
            
            st.dataframe(
                df_map_display,
                column_config={
                    "city": st.column_config.TextColumn("Ciudad"),
                    "region": st.column_config.TextColumn("Región"),
                    "temp_promedio": st.column_config.NumberColumn("Temp °C", format="%.1f"),
                    "sensacion_termica": st.column_config.NumberColumn("Sensación °C", format="%.1f"),
                    "humedad_promedio": st.column_config.NumberColumn("Humedad %", format="%.1f"),
                    "viento_promedio": st.column_config.NumberColumn("Viento km/h", format="%.1f"),
                    "num_alertas": st.column_config.NumberColumn("🚨 Alertas", format="%d")
                },
                hide_index=True,
                use_container_width=True
            )
        
        st.markdown("---")
        
        # ==========================================
        # SECCIÓN 4: GRÁFICAS DE SERIES DE TIEMPO
        # ==========================================
        
        st.subheader("📈 Evolución Temporal de Variables Meteorológicas")
        
        query_timeseries = f"""
        SELECT 
            fws.ts,
            dl.city,
            dl.region,
            fws.temperature,
            fws.feels_like,
            fws.humidity,
            fws.pressure,
            fws.wind_speed,
            fws.precipitation,
            fws.status
        FROM stream.fact_weather_stream fws
        JOIN stream.dim_location dl ON fws.location_id = dl.location_id
        WHERE fws.ts > NOW() AT TIME ZONE 'UTC' - INTERVAL '{minutes} minutes'
        ORDER BY fws.ts ASC
        """
        
        df_ts = fetch_data(query_timeseries)
        
        if not df_ts.empty:
            # Aplicar filtro de región
            if "Todas" not in region_filter:
                if "Caribe" in region_filter and "Interior" not in region_filter:
                    df_ts = df_ts[df_ts['region'].isin(['Atlántico', 'Bolívar', 'Magdalena', 'Cesar', 'Córdoba'])]
                elif "Interior" in region_filter and "Caribe" not in region_filter:
                    df_ts = df_ts[~df_ts['region'].isin(['Atlántico', 'Bolívar', 'Magdalena', 'Cesar', 'Córdoba'])]
            
            tab1, tab2, tab3, tab4 = st.tabs(["🌡️ Temperatura", "💧 Humedad", "💨 Viento", "📊 Comparación"])
            
            with tab1:
                # Gráfica de temperatura y sensación térmica
                fig_temp = go.Figure()
                
                for city in df_ts['city'].unique():
                    df_city = df_ts[df_ts['city'] == city]
                    
                    # Temperatura real
                    fig_temp.add_trace(go.Scatter(
                        x=df_city['ts'],
                        y=df_city['temperature'],
                        name=f"{city} - Temp",
                        mode='lines',
                        line=dict(width=2)
                    ))
                    
                    # Sensación térmica (línea punteada)
                    fig_temp.add_trace(go.Scatter(
                        x=df_city['ts'],
                        y=df_city['feels_like'],
                        name=f"{city} - Sensación",
                        mode='lines',
                        line=dict(width=1, dash='dot'),
                        opacity=0.6
                    ))
                
                # Líneas de referencia
                fig_temp.add_hline(y=35, line_dash="dash", line_color="red", 
                                  annotation_text="Calor Extremo (35°C)")
                fig_temp.add_hline(y=28, line_dash="dash", line_color="orange", 
                                  annotation_text="Promedio Caribe (28°C)")
                
                fig_temp.update_layout(
                    title="Temperatura y Sensación Térmica",
                    xaxis_title="Tiempo",
                    yaxis_title="Temperatura (°C)",
                    hovermode='x unified',
                    height=400
                )
                
                st.plotly_chart(fig_temp, use_container_width=True, key=f"temp_chart_{iteration}")
            
            with tab2:
                # Gráfica de humedad
                fig_humid = px.line(
                    df_ts,
                    x='ts',
                    y='humidity',
                    color='city',
                    title='Humedad Relativa',
                    labels={'ts': 'Tiempo', 'humidity': 'Humedad (%)', 'city': 'Ciudad'}
                )
                fig_humid.update_traces(mode='lines')
                fig_humid.update_layout(hovermode='x unified', height=400)
                st.plotly_chart(fig_humid, use_container_width=True, key=f"humid_chart_{iteration}")
            
            with tab3:
                # Gráfica de viento
                fig_wind = px.line(
                    df_ts,
                    x='ts',
                    y='wind_speed',
                    color='city',
                    title='Velocidad del Viento',
                    labels={'ts': 'Tiempo', 'wind_speed': 'Viento (km/h)', 'city': 'Ciudad'}
                )
                fig_wind.add_hline(y=40, line_dash="dash", line_color="red", 
                                  annotation_text="Viento Fuerte (40 km/h)")
                fig_wind.update_traces(mode='lines')
                fig_wind.update_layout(hovermode='x unified', height=400)
                st.plotly_chart(fig_wind, use_container_width=True, key=f"wind_chart_{iteration}")
            
            with tab4:
                # Comparación Caribe vs Interior
                df_comparison = df_ts.copy()
                df_comparison['zona'] = df_comparison['region'].apply(
                    lambda x: 'Caribe' if x in ['Atlántico', 'Bolívar', 'Magdalena', 'Cesar', 'Córdoba'] else 'Interior'
                )
                
                df_comp_agg = df_comparison.groupby(['ts', 'zona']).agg({
                    'temperature': 'mean',
                    'humidity': 'mean'
                }).reset_index()
                
                fig_comparison = go.Figure()
                
                for zona in df_comp_agg['zona'].unique():
                    df_zona = df_comp_agg[df_comp_agg['zona'] == zona]
                    fig_comparison.add_trace(go.Scatter(
                        x=df_zona['ts'],
                        y=df_zona['temperature'],
                        name=zona,
                        mode='lines',
                        line=dict(width=3)
                    ))
                
                fig_comparison.update_layout(
                    title="Comparación: Temperatura Caribe vs Interior",
                    xaxis_title="Tiempo",
                    yaxis_title="Temperatura Promedio (°C)",
                    hovermode='x unified',
                    height=400
                )
                
                st.plotly_chart(fig_comparison, use_container_width=True, key=f"comp_chart_{iteration}")
        
        st.markdown("---")
        
        # ==========================================
        # SECCIÓN 5: ANÁLISIS Y DISTRIBUCIONES
        # ==========================================
        
        st.subheader("📊 Análisis Estadístico y Distribuciones")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Histograma de temperaturas
            if not df_ts.empty:
                fig_hist = px.histogram(
                    df_ts,
                    x='temperature',
                    nbins=30,
                    title='Distribución de Temperaturas',
                    labels={'temperature': 'Temperatura (°C)', 'count': 'Frecuencia'},
                    color_discrete_sequence=['#FF6B6B']
                )
                fig_hist.add_vline(x=df_ts['temperature'].mean(), line_dash="dash", 
                                  line_color="blue", annotation_text="Media")
                st.plotly_chart(fig_hist, use_container_width=True, key=f"hist_chart_{iteration}")
        
        with col2:
            # Box plot por ciudad
            if not df_ts.empty:
                fig_box = px.box(
                    df_ts,
                    x='city',
                    y='temperature',
                    title='Dispersión de Temperaturas por Ciudad',
                    labels={'city': 'Ciudad', 'temperature': 'Temperatura (°C)'},
                    color='city'
                )
                st.plotly_chart(fig_box, use_container_width=True, key=f"box_chart_{iteration}")
        
        st.markdown("---")
        
        # ==========================================
        # SECCIÓN 6: DETECCIÓN DE OUTLIERS/ANOMALÍAS
        # ==========================================
        
        st.subheader("🔍 Detección de Anomalías (Outliers)")
        
        if not df_ts.empty:
            df_anomalies = detect_anomalies(df_ts.copy(), 'temperature', anomaly_threshold)
            
            anomaly_count = df_anomalies['is_anomaly'].sum()
            total_readings = len(df_anomalies)
            anomaly_pct = (anomaly_count / total_readings * 100) if total_readings > 0 else 0
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("🚨 Anomalías Detectadas", int(anomaly_count))
            with col2:
                st.metric("📊 Total de Lecturas", total_readings)
            with col3:
                st.metric("📈 Porcentaje de Anomalías", f"{anomaly_pct:.2f}%")
            
            # Gráfica de scatter con anomalías
            fig_anomalies = px.scatter(
                df_anomalies,
                x='ts',
                y='temperature',
                color='is_anomaly',
                hover_data=['city', 'z_score'],
                title=f'Detección de Anomalías (Umbral z-score: {anomaly_threshold})',
                labels={
                    'ts': 'Tiempo',
                    'temperature': 'Temperatura (°C)',
                    'is_anomaly': 'Es Anomalía'
                },
                color_discrete_map={True: '#FF0000', False: '#4169E1'}
            )
            st.plotly_chart(fig_anomalies, use_container_width=True, key=f"anomaly_chart_{iteration}")
            
            # Tabla de anomalías más extremas
            if anomaly_count > 0:
                st.markdown("#### 🔴 Anomalías Más Extremas")
                top_anomalies = df_anomalies[df_anomalies['is_anomaly']].nlargest(10, 'z_score')[
                    ['ts', 'city', 'temperature', 'feels_like', 'z_score', 'status']
                ]
                st.dataframe(
                    top_anomalies,
                    column_config={
                        "ts": st.column_config.DatetimeColumn("Fecha/Hora", format="DD/MM/YY HH:mm:ss"),
                        "city": "Ciudad",
                        "temperature": st.column_config.NumberColumn("Temp (°C)", format="%.1f"),
                        "feels_like": st.column_config.NumberColumn("Sensación (°C)", format="%.1f"),
                        "z_score": st.column_config.NumberColumn("Z-Score", format="%.2f"),
                        "status": "Estado"
                    },
                    hide_index=True,
                    use_container_width=True
                )
        
        st.markdown("---")
        
        # ==========================================
        # SECCIÓN 7: DATOS EN TIEMPO REAL (TABLA)
        # ==========================================
        
        st.subheader("📋 Lecturas Más Recientes")
        
        if not df_ts.empty:
            # Últimas 20 lecturas
            df_recent = df_ts.sort_values('ts', ascending=False).head(20)
            
            st.dataframe(
                df_recent[['ts', 'city', 'temperature', 'feels_like', 'humidity', 
                          'wind_speed', 'precipitation', 'status']],
                column_config={
                    "ts": st.column_config.DatetimeColumn("Fecha/Hora", format="DD/MM/YY HH:mm:ss"),
                    "city": "Ciudad",
                    "temperature": st.column_config.NumberColumn("Temp (°C)", format="%.1f"),
                    "feels_like": st.column_config.NumberColumn("Sensación (°C)", format="%.1f"),
                    "humidity": st.column_config.NumberColumn("Humedad (%)", format="%.1f"),
                    "wind_speed": st.column_config.NumberColumn("Viento (km/h)", format="%.1f"),
                    "precipitation": st.column_config.NumberColumn("Lluvia (mm)", format="%.2f"),
                    "status": "Estado"
                },
                hide_index=True,
                use_container_width=True
            )
            
            # Botón de descarga
            csv = df_ts.to_csv(index=False)
            st.download_button(
                label="📥 Descargar Datos Completos (CSV)",
                data=csv,
                file_name=f'climacaribe_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
                mime='text/csv',
            )
        
        # ==========================================
        # FOOTER
        # ==========================================
        
        st.markdown("---")
        
        ultima_actualizacion = None
        if 'ultima_actualizacion' in df_kpis.columns:
            ultima_actualizacion = df_kpis.iloc[0]['ultima_actualizacion']
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if pd.notnull(ultima_actualizacion):
                st.caption(f"🕒 Última actualización: {ultima_actualizacion.strftime('%H:%M:%S')}")
            else:
                st.caption("🕒 Última actualización: Sin datos recientes")
        
        with col2:
            ultima_lectura = None
            if 'ultima_actualizacion' in df_kpis.columns:
                ultima_lectura = df_kpis.iloc[0]['ultima_actualizacion']
            if pd.notnull(ultima_lectura):
                st.caption(f"📈 Última lectura BD: {ultima_lectura.strftime('%H:%M:%S')}")
            else:
                st.caption("📈 Última lectura BD: Sin datos recientes")
        
        with col3:
            auto_activo = st.session_state.get("auto_update", True)
            if auto_activo:
                st.caption("🔄 Auto-actualización: Activa")
            else:
                st.caption("🔄 Auto-actualización: Inactiva")       
        with col4:
            iteracion = st.session_state.get("iteracion", 0)
            st.caption(f"📊 Iteración: {iteracion}")
    
    # Auto-refresh logic
    if auto_refresh:
        time.sleep(30)
    else:
        break

# ==========================================
# INSTRUCCIONES INICIALES
# ==========================================

if not auto_refresh and iteration == 1:
    st.info("""
    💡 **Instrucciones:**
    - Activa "Auto-actualización" en la barra lateral para refrescar cada 30 segundos
    - Ajusta el rango de tiempo para ver más o menos datos históricos
    - Usa los filtros de región para enfocarte en el Caribe o el Interior
    - El dashboard detecta automáticamente outliers basados en el umbral de z-score

    """)


