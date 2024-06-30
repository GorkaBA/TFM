import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Parámetros de la API y InfluxDB
api_url = "https://apidatos.ree.es/es/datos/demanda/evolucion?end_date=2024-05-19T23:59&time_trunc=day&start_date=2024-05-15T04:00"
influxdb_url = "http://localhost:8086"
influxdb_token = "MH7gG-ayy-Oum10exnUj9eXLnMRXVSna2aSmRcDHTXt2E4iFZLE1Ozms9McDA5KHFYhpZw823A5Onbma2UNbbg=="
influxdb_org = "tfm"
influxdb_bucket = "test"

# Obtener datos de la API
response = requests.get(api_url)
data = response.json()

# Verificar si la solicitud fue exitosa
if response.status_code != 200:
    print("Error al obtener datos de la API:", response.status_code)
    print("Mensaje:", response.text)
else:
    # Conectar a InfluxDB
    client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Procesar y escribir datos en InfluxDB
    for item in data['included']:
        for value in item['attributes']['values']:
            point = Point("demanda_evolucion") \
                .tag("title", item["attributes"]["title"]) \
                .field("value", value["value"]) \
                .time(value["datetime"], WritePrecision.S)
            write_api.write(bucket=influxdb_bucket, record=point)

    # Cerrar conexión
    client.close()
    print("Datos escritos en InfluxDB con éxito.")
