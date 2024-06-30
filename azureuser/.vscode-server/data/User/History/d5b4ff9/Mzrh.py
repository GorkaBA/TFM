import requests
import pandas as pd
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from config import nombres, influxdb_token, influxdb_bucket, influxdb_org, influxdb_url
from balance_carga import consulta_balance_electrico, balance_comunidades
from prophet import Prophet
import logging

# Parámetros de InfluxDB
client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Descargar datos históricos del último año
def descargar_datos_historicos(tipo_demanda):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    api_url = f"https://apidatos.ree.es/es/datos/{tipo_demanda[0]}/{tipo_demanda[1]}?end_date={end_date.strftime('%Y-%m-%dT%H:%M:%S')}&time_trunc=day&start_date={start_date.strftime('%Y-%m-%dT%H:%M:%S')}"
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
    df['ds'] = pd.to_datetime(df['ds'])  # Asegurarse de que la columna 'ds' sea de tipo datetime
    return df

# Predecir la demanda de la próxima semana
def predecir_demanda(df):
    model = Prophet()
    model.fit(df)
    
    future = model.make_future_dataframe(periods=7)
    forecast = model.predict(future)
    
    return forecast[['ds', 'yhat']].tail(7)

# Guardar predicciones en InfluxDB con un nombre diferente
def guardar_predicciones(predicciones, tipo_demanda):
    for index, row in predicciones.iterrows():
        point = Point(f"{tipo_demanda[1]}_prediccion") \
            .tag("Categoria", tipo_demanda[0]) \
            .field("Prediccion", row['yhat']) \
            .time(row['ds'], WritePrecision.S)
        write_api.write(bucket=influxdb_bucket, record=point)
    print("Predicciones guardadas en InfluxDB con éxito.")

# Obtener el primer elemento de nombres
tipo_demanda = nombres[0]

# Descargar datos históricos
df = descargar_datos_historicos(tipo_demanda)
print("Datos históricos descargados.")

# Predecir demanda para la próxima semana
predicciones = predecir_demanda(df)
print("Predicciones realizadas.")

# Guardar predicciones en InfluxDB
guardar_predicciones(predicciones, tipo_demanda)
print("Proceso completado.")
