import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# Parámetros de InfluxDB
influxdb_url = "http://localhost:8086"
influxdb_token = "YOUR_INFLUXDB_TOKEN"
influxdb_org = "YOUR_INFLUXDB_ORG"
influxdb_bucket = "YOUR_INFLUXDB_BUCKET"

# Conectar a InfluxDB
client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
write_api = client.write_api(write_options=SYNCHRONOUS)

def consulta_balance_electrico(anno):
    try:
        api_url = f"https://apidatos.ree.es/es/datos/balance/balance-electrico?end_date={anno}-01-01T00:00&time_trunc=day&start_date={anno}-12-02T00:00"
        
        print(f"Consultando API: {api_url}")
        # Obtener datos de la API
        response = requests.get(api_url)
        data = response.json()

        # Verificar si la solicitud fue exitosa
        if response.status_code != 200:
            print(f"Error al obtener datos de la API:", response.status_code)
            print("Mensaje:", response.text)
            return
        else:
            for item in data['included']:
                for content in item['attributes']['content']:
                    for value in content['attributes']['values']:
                        # Convertir el valor a float y formatear a 3 decimales
                        try:
                            valor_decimal = float("{:.3f}".format(float(value["value"])))
                        except ValueError as e:
                            print(f"Error al convertir el valor: {value['value']} a float: {e}")
                            continue

                        point = Point("balance_electrico") \
                            .tag("type", item["type"]) \
                            .tag("title", content["attributes"]["title"]) \
                            .field("value", valor_decimal) \
                            .field("percentage", value["percentage"]) \
                            .time(value["datetime"], WritePrecision.S)
                        write_api.write(bucket=influxdb_bucket, record=point)

            print(f"Datos de balance eléctrico escritos en InfluxDB con éxito.")
    except Exception as e:
        print(f"Error procesando la consulta de balance eléctrico: {e}")

consulta_balance_electrico()
