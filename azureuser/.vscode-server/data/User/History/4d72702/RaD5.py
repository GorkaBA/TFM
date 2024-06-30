import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import time

# Parámetros de InfluxDB
influxdb_url = "http://localhost:8086"
influxdb_token = "MH7gG-ayy-Oum10exnUj9eXLnMRXVSna2aSmRcDHTXt2E4iFZLE1Ozms9McDA5KHFYhpZw823A5Onbma2UNbbg=="
influxdb_org = "tfm"
influxdb_bucket = "test"

# Conectar a InfluxDB
client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
write_api = client.write_api(write_options=SYNCHRONOUS)

for anno in range(2011, 2025):
    # Parámetros de la API
    api_url = f"https://apidatos.ree.es/es/datos/demanda/evolucion?end_date={anno}-12-31T23:59&time_trunc=day&start_date={anno}-01-01T04:00"

    # Obtener datos de la API
    response = requests.get(api_url)
    data = response.json()

    # Verificar si la solicitud fue exitosa
    if response.status_code != 200:
        print(f"Error al obtener datos de la API para el año {anno}:", response.status_code)
        print("Mensaje:", response.text)
    else:
        # Procesar y escribir datos en InfluxDB
        for item in data['included']:
            for value in item['attributes']['values']:
                
                # Convertir el valor a float y formatear a 3 decimales
                try:
                    valor_decimal = float("{:.3f}".format(float(value["value"])))
                except ValueError as e:
                    print(f"Error al convertir el valor: {value['value']} a float: {e}")
                    continue

                # print(item['attributes'])
                # print(valor_decimal)
                point = Point("demanda_evolucion") \
                    .tag("title", item["attributes"]["title"]) \
                    .field("value", valor_decimal) \
                    .time(value["datetime"], WritePrecision.S)
                write_api.write(bucket=influxdb_bucket, record=point)

        print(f"Datos para el año {anno} escritos en InfluxDB con éxito.")

# Cerrar conexión
client.close()
