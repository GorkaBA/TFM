from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from sklearn.linear_model import LinearRegression
import numpy as np
from config import nombres, influxdb_token, influxdb_bucket, influxdb_org, influxdb_url, measurement

# Crear cliente InfluxDB
client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)

# Calcular fecha de inicio (hoy - 1 año)
end_date = datetime.utcnow()
start_date = end_date - timedelta(days=365)

# Formato de fecha para consulta a InfluxDB
start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

# Consultar datos desde InfluxDB
query = f'from(bucket:"{influxdb_bucket}") |> range(start: {start_date_str}, stop: {end_date_str}) |> filter(fn: (r) => r["_measurement"] == "{measurement}")'
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

# Construcción del modelo de predicción
timestamps_numeric = np.array([t.timestamp() for t in timestamps]).reshape(-1, 1)

model = LinearRegression()
model.fit(timestamps_numeric, values)

# Predicción de la semana siguiente
next_week_timestamps = []
for i in range(7):
    next_day_timestamp = end_date + timedelta(days=i+1)
    next_week_timestamps.append(next_day_timestamp.timestamp())

next_week_timestamps_numeric = np.array(next_week_timestamps).reshape(-1, 1)
predicted_values = model.predict(next_week_timestamps_numeric)

# Escribir los puntos de predicción en InfluxDB
write_api = client.write_api(write_options=SYNCHRONOUS)

for i, timestamp in enumerate(next_week_timestamps):
    point = Point(measurement=measurement) \
        .time(int(timestamp * 1e9)) \
        .field("predicted_value", predicted_values[i]) \
        .tag("prediction", "next_week")

    write_api.write(bucket=influxdb_bucket, org=influxdb_org, record=point)

# Cerrar cliente InfluxDB
client.close()
