from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from sklearn.linear_model import LinearRegression
import numpy as np
from config import influxdb_token, influxdb_bucket, influxdb_org, influxdb_url, categoria, tema
from dateutil.relativedelta import relativedelta

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
query = f'from(bucket:"{influxdb_bucket}") |> range(start: {start_date_str}, stop: {end_date_str}) |> filter(fn: (r) => r["Categoria"] == "{categoria}" and r["Tema"] == "{tema}")'
result = client.query_api().query(org=influxdb_org, query=query)

# Procesar resultados y almacenar en una lista de tuplas
data = []
for table in result:
    for record in table.records:
        timestamp = record["_time"]
        value = record["_value"]
        data.append((timestamp, value))

# Preprocesamiento de los datos
timestamps = []
values = []

for timestamp, value in data:
    timestamps.append(timestamp)
    values.append(value)

# Añadir el valor del mismo día del año anterior como característica adicional
timestamps_numeric = np.array([t.timestamp() for t in timestamps]).reshape(-1, 1)
values = np.array(values)

# Obtener valores del mismo día del año anterior
timestamps_prev_year = [datetime.strptime(t, '%Y-%m-%dT%H:%M:%SZ') - relativedelta(years=1) for t in timestamps]
timestamps_prev_year_numeric = np.array([t.timestamp() for t in timestamps_prev_year]).reshape(-1, 1)

# Concatenar características
timestamps_combined = np.concatenate((timestamps_numeric, timestamps_prev_year_numeric), axis=1)

# Construcción del modelo de predicción
model = LinearRegression()
model.fit(timestamps_combined, values)

# Predicción de la semana siguiente
next_week_timestamps = []
for i in range(7):
    next_day_timestamp = end_date + timedelta(days=i+1)
    next_week_timestamps.append(next_day_timestamp.timestamp())

# Obtener valores del mismo día del año anterior para la semana siguiente
next_week_timestamps_prev_year = [next_day_timestamp - timedelta(days=365) for next_day_timestamp in next_week_timestamps]
next_week_timestamps_numeric = np.array(next_week_timestamps).reshape(-1, 1)
next_week_timestamps_prev_year_numeric = np.array([t.timestamp() for t in next_week_timestamps_prev_year]).reshape(-1, 1)
next_week_timestamps_combined = np.concatenate((next_week_timestamps_numeric, next_week_timestamps_prev_year_numeric), axis=1)

predicted_values = model.predict(next_week_timestamps_combined)

# Escribir los puntos de predicción en InfluxDB
for i, timestamp in enumerate(next_week_timestamps):
    point = Point("evolucion") \
        .tag("Categoria", categoria) \
        .tag("Tema", tema) \
        .tag("Tipo", "Prediccion") \
        .time(int(timestamp * 1e9)) \
        .field("Prediccion", predicted_values[i])

    write_api.write(bucket=influxdb_bucket, record=point)

# Cerrar cliente InfluxDB
client.close()
