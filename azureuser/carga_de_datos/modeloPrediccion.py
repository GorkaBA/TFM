import requests
import pandas as pd
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from config import nombres, influxdb_token, influxdb_bucket, influxdb_org, influxdb_url
from prophet import Prophet
import logging

# Parámetros de InfluxDB
client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Descargar datos históricos del último año
def descargar_datos_historicos():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    api_url = f"https://apidatos.ree.es/es/datos/demanda/evolucion?end_date={end_date.strftime('%Y-%m-%dT%H:%M:%S')}&time_trunc=day&start_date={start_date.strftime('%Y-%m-%dT%H:%M:%S')}"
    response = requests.get(api_url)
    data = response.json()
    registros = []

    for item in data['included']:
        for value in item['attributes']['values']:
            registros.append({
                'ds': value['datetime'],
                'y': float(value['value'])
            })
    
    df = pd.DataFrame(registros)
    df['ds'] = pd.to_datetime(df['ds'], utc=True, errors='coerce')  # Convertir a datetime con UTC
    df['ds'] = df['ds'].dt.tz_convert('UTC').dt.tz_localize(None)  # Asegurar que las fechas estén en UTC y sin info de zona horaria
    return df

# Predecir la demanda de la próxima semana
def predecir_demanda(df):
    model = Prophet(daily_seasonality=True)
    model.fit(df)
    
    # Crear un DataFrame futuro a partir de mañana
    tomorrow = datetime.now() + timedelta(days=1)
    future_dates = [tomorrow + timedelta(days=i) for i in range(10)]
    future = pd.DataFrame(future_dates, columns=['ds'])
    
    forecast = model.predict(future)
    
    return forecast[['ds', 'yhat']]

# Guardar predicciones en InfluxDB con un nombre diferente
def guardar_predicciones(predicciones, tipo_demanda):
    x = 1
    for index, row in predicciones.iterrows():
        print(x)
        print(row)
        x = x+1
        point = Point(f"Prediccion_demanda2") \
            .tag("Categoria", tipo_demanda[0]) \
            .field("Prediccion", row['yhat']) \
            .time(row['ds'], WritePrecision.S)
        write_api.write(bucket=influxdb_bucket, record=point)
    print("Predicciones guardadas en InfluxDB con éxito.")

# Obtener el primer elemento de nombres
tipo_demanda = nombres[0]

# Descargar datos históricos
df = descargar_datos_historicos()
print("Datos históricos descargados.")

# Predecir demanda para la próxima semana
predicciones = predecir_demanda(df)
print("Predicciones realizadas.")

# Guardar predicciones en InfluxDB
guardar_predicciones(predicciones, tipo_demanda)
print("Proceso completado.")
