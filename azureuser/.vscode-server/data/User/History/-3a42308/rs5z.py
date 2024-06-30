import pandas as pd
from datetime import datetime, timedelta
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

token = "MH7gG-ayy-Oum10exnUj9eXLnMRXVSna2aSmRcDHTXt2E4iFZLE1Ozms9McDA5KHFYhpZw823A5Onbma2UNbbg=="
org = "tfm"
url = "http://influxdb:8086"
bucket="test"

# Ruta al archivo CSV
ruta_csv = '/home/azureuser/archivo.csv'

# Cargar el archivo CSV como un DataFrame
df = pd.read_csv(ruta_csv)

# Cliente de InfluxDB y API de escritura
client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Función para convertir el tiempo a formato de tiempo Unix
def to_unix_time(time_str):
    return int(datetime.strptime(time_str, '%Y-%m-%d').timestamp()) * 1000000000

# Iterar sobre las filas del DataFrame y escribir los puntos en InfluxDB
puntos_acumulados = []
contador = 0
for index, row in df.iterrows():
    timestamp_influxdb = to_unix_time(row['Fecha'])  # Convertir el tiempo a formato Unix
    point = (Point("vela")
             .field(row['Comunidad Autónoma'], row['Valor'])  # Campo: Comunidad Autónoma
             .tag("sensor", row['Sensor'])  # Tag: Sensor
             .time(timestamp_influxdb, WritePrecision.NS)
             )
    puntos_acumulados.append(point)
    contador += 1
    # Si hay suficientes puntos acumulados o ha pasado suficiente tiempo, escribir en InfluxDB
    if len(puntos_acumulados) >= 10000:
        write_api.write(bucket=bucket, org=org, record=puntos_acumulados)
        print('Carga Realizada!!!')
        print(contador)
        puntos_acumulados = []
        time.sleep(1)

# Escribir los puntos restantes en InfluxDB
if puntos_acumulados:
    write_api.write(bucket=bucket, org=org, record=puntos_acumulados)
    print('Carga Finalizada!!!')
    print(contador)