from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from config import nombres, influxdb_token, influxdb_bucket, influxdb_org, influxdb_url

# Crear cliente InfluxDB
client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)

# Calcular fecha de inicio (hoy - 1 año)
end_date = datetime.utcnow()
start_date = end_date - timedelta(days=365)

# Formato de fecha para consulta a InfluxDB
start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

# Consultar datos desde InfluxDB
query = f'from(bucket:"{influxdb_bucket}") |> range(start: {start_date_str}, stop: {end_date_str}) |> filter(fn: (r) => r["_measurement"] == "{_measurement}")'
result = client.query_api().query(org=influxdb_org, query=query)

# Procesar resultados
for table in result:
    for record in table.records:
        print(f'Timestamp: {record["_time"]}, Value: {record["_value"]}')

# Cerrar cliente InfluxDB
client.close()