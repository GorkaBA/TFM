from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder
import numpy as np
from config import influxdb_token, influxdb_bucket, influxdb_org, influxdb_url, categoria, tema
from dateutil.relativedelta import relativedelta

influxdb_bucket = "raw_co2_data"
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
timestamps_prev_year = [datetime.fromtimestamp(t.timestamp() - 365 * 24 * 60 * 60) for t in timestamps]
timestamps_prev_year_numeric = np.array([t.timestamp() for t in timestamps_prev_year]).reshape(-1, 1)

# Obtener día de la semana como una característica categórica (0=lunes, 1=martes, ..., 6=domingo)
weekdays = np.array([t.weekday() for t in timestamps]).reshape(-1, 1)

# Codificar el día de la semana como variables dummy
encoder = OneHotEncoder(categories='auto', sparse=False)
weekdays_encoded = encoder.fit_transform(weekdays)

# Concatenar características
timestamps_combined = np.concatenate((timestamps_numeric, timestamps_prev_year_numeric, weekdays_encoded), axis=1)

# Construcción del modelo de predicción
model = LinearRegression()
model.fit(timestamps_combined, values)

# Predicción de la semana siguiente
next_week_timestamps = []
for i in range(7):
    next_day_timestamp = end_date + timedelta(days=i+1)
    next_week_timestamps.append(next_day_timestamp)

# Obtener valores del mismo día del año anterior para la semana siguiente
next_week_timestamps_prev_year = [next_day_timestamp - timedelta(days=365) for next_day_timestamp in next_week_timestamps]
next_week_timestamps_numeric = np.array([t.timestamp() for t in next_week_timestamps]).reshape(-1, 1)
next_week_timestamps_prev_year_numeric = np.array([t.timestamp() for t in next_week_timestamps_prev_year]).reshape(-1, 1)
next_week_weekdays = np.array([t.weekday() for t in next_week_timestamps]).reshape(-1, 1)
next_week_weekdays_encoded = encoder.transform(next_week_weekdays)
next_week_timestamps_combined = np.concatenate((next_week_timestamps_numeric, next_week_timestamps_prev_year_numeric, next_week_weekdays_encoded), axis=1)

predicted_values = model.predict(next_week_timestamps_combined)

# Escribir los puntos de predicción en InfluxDB
for i, timestamp in enumerate(next_week_timestamps):
    point = Point("evolucion") \
        .tag("Categoria", categoria) \
        .tag("Tema", tema) \
        .tag("Tipo", "Prediccion") \
        .time(int(timestamp.timestamp() * 1e9)) \
        .field("Prediccion", predicted_values[i])

    write_api.write(bucket=influxdb_bucket, record=point)

# Cerrar cliente InfluxDB
client.close()
