from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Configura la conexión a InfluxDB
influxdb_url = "http://localhost:8086"
influxdb_token = "MH7gG-ayy-Oum10exnUj9eXLnMRXVSna2aSmRcDHTXt2E4iFZLE1Ozms9McDA5KHFYhpZw823A5Onbma2UNbbg=="
influxdb_org = "tfm"
influxdb_bucket = "raw_api_data"

# Inicializa el cliente InfluxDB
client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)

# Obtén una referencia al punto de escritura
write_api = client.write_api(write_options=SYNCHRONOUS)

# Define el query para eliminar puntos donde Tipo sea igual a 'predicción'
delete_query = f'delete from "{influxdb_bucket}" where Tipo = \'Prediccion\''

# Ejecuta la consulta de eliminación
delete_result = client.query_api().query(delete_query)

print(f"Eliminados {delete_result}")
