from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder
import numpy as np
from config import influxdb_token, influxdb_bucket, influxdb_org, influxdb_url, categoria, tema, comunidades
from dateutil.relativedelta import relativedelta

influxdb_bucket = "etl_data"
categoria = "demanda"
tema = "evolucion_general"
measurement = "prediccion"

# Crear cliente InfluxDB
client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Calcular fecha de inicio (hoy - 2 años)
end_date = datetime.utcnow()
start_date = end_date - timedelta(days=365 * 2)  # 2 años atrás

# Formato de fecha para consulta a InfluxDB
start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

# Consultar datos desde InfluxDB
query = f'''
from(bucket:"{influxdb_bucket}") 
  |> range(start: {start_date_str}, stop: {end_date_str}) 
  |> filter(fn: (r) => r["_measurement"] == "mensual" and r["_field"] == "CO2_ajustado_solo_generacion" and r["Comunidad"] == "Extremadura")
'''
result = client.query_api().query(org=influxdb_org, query=query)

# Procesar resultados y almacenar en una lista de tuplas
data = []
for table in result:
    for record in table.records:
        timestamp = record["_time"]
        value = record["_value"]
        data.append((timestamp, value))

# Imprimir los datos descargados
for timestamp, value in data:
    print(f'Timestamp: {timestamp}, Value: {value}')

# Preprocesamiento de los datos
timestamps = []
values = []

for timestamp, value in data:
    timestamps.append(timestamp)
    values.append(value)

# Obtener el último timestamp y calcular los dos siguientes meses
last_timestamp = max(timestamps)
next_months = [
    (last_timestamp + relativedelta(months=1)).replace(day=1, hour=1, minute=0, second=0, microsecond=0),
    (last_timestamp + relativedelta(months=2)).replace(day=1, hour=1, minute=0, second=0, microsecond=0)
]

# Añadir el valor del mismo mes del año anterior como característica adicional
timestamps_numeric = np.array([t.timestamp() for t in timestamps]).reshape(-1, 1)
values = np.array(values)

# Obtener valores del mismo mes del año anterior
timestamps_prev_year = [datetime.fromtimestamp(t.timestamp() - 365 * 24 * 60 * 60) for t in timestamps]
timestamps_prev_year_numeric = np.array([t.timestamp() for t in timestamps_prev_year]).reshape(-1, 1)

# Obtener el mes como una característica categórica (1=enero, 2=febrero, ..., 12=diciembre)
months = np.array([t.month for t in timestamps]).reshape(-1, 1)

# Codificar el mes como variables dummy, asegurándonos de que se incluyen todos los meses
encoder = OneHotEncoder(categories=[np.arange(1, 13)], sparse_output=False)
months_encoded = encoder.fit_transform(months)

# Concatenar características
timestamps_combined = np.concatenate((timestamps_numeric, timestamps_prev_year_numeric, months_encoded), axis=1)

# Construcción del modelo de predicción con restricción de no negatividad
model = LinearRegression(positive=True)
model.fit(timestamps_combined, values)

predicted_values = []

# Predecir para cada uno de los siguientes dos meses
for next_month in next_months:
    next_month_prev_year = next_month - relativedelta(years=1)
    next_month_timestamps_numeric = np.array([next_month.timestamp()]).reshape(-1, 1)
    next_month_prev_year_numeric = np.array([next_month_prev_year.timestamp()]).reshape(-1, 1)
    next_months_array = np.array([next_month.month]).reshape(-1, 1)
    next_months_encoded = encoder.transform(next_months_array)
    next_month_combined = np.concatenate((next_month_timestamps_numeric, next_month_prev_year_numeric, next_months_encoded), axis=1)
    
    predicted_value = model.predict(next_month_combined)
    predicted_values.append(predicted_value)

# Asegurarse de que los valores predichos no sean negativos
predicted_values = np.clip(predicted_values, a_min=0, a_max=None)

# Escribir los puntos de predicción en InfluxDB
for i, timestamp in enumerate(next_months):
    point = Point("mensual") \
        .tag("Comunidad", "Prueba2") \
        .time(int(timestamp.timestamp() * 1e9)) \
        .field("CO2_ajustado_solo_generacion_Prediccion", predicted_values[i][0])

    write_api.write(bucket=influxdb_bucket, record=point)

# Cerrar cliente InfluxDB
client.close()
