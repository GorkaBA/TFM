import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
from config import geo_names, influxdb_token, influxdb_bucket, influxdb_org, influxdb_url

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

def balance_comunidades(anno, tipo_demanda):
    for geo_buckle in range(1, 22):
        geo_id = geo_buckle
        if geo_id == 1:
            # Comunidad Ceuta
            geo_id = 8744
        elif geo_id == 2:
            # Comunidad Melilla
            geo_id = 8745
        elif geo_id == 3:
            # Comunidad Islas Baleares
            geo_id = 8743
        elif geo_id == 12:
            # Comunidad Canarias
            geo_id = 8742
        
        # Asignar nombres a geo_id específicos
        if geo_buckle in geo_names:
            geo_name = geo_names[geo_buckle]
        else:
            geo_name = f"Comunidad_{geo_buckle}"
        demanda_ccaa(anno, geo_name, geo_id, tipo_demanda)


def demanda_ccaa(anno, geo_name, geo_id, tipo_demanda):
    try:
        # Parámetros de la API
        api_url = f"https://apidatos.ree.es/es/datos/{tipo_demanda[0]}/{tipo_demanda[1]}?end_date={anno}-12-31T23:59&time_trunc=month&start_date={anno}-01-01T04:00&geo_trunc=electric_system&geo_limit=ccaa&geo_ids={geo_id}"

        # Obtener datos de la API
        response = requests.get(api_url)
        data = response.json()

        # Verificar si la solicitud fue exitosa
        if response.status_code != 200:
            print(f"Error al obtener datos de la API para el año {anno}, geo_id {geo_id}, BALANCE:", response.status_code)
            print("Mensaje:", response.text)
            return
        else:
            # Verificar estructura de datos
            if 'included' not in data:
                print(f"Estructura de datos inesperada para el año {anno}, geo_id {geo_id}: 'included' no encontrado BALANCE")
                print(data)
                return

            for item in data['included']:
                if 'attributes' not in item or 'values' not in item['attributes']:
                    print(f"Estructura de datos inesperada en item: {item} BALANCE")
                    continue

                for value in item['attributes']['values']:
                    # Convertir el valor a float y formatear a 3 decimales
                    try:
                        valor_decimal = float("{:.3f}".format(float(value["value"])))
                    except ValueError as e:
                        print(f"Error al convertir el valor: {value['value']} a float: {e}")
                        continue

                    # Agregar el nombre de la comunidad al título
                    title_with_geo = f"{item['attributes']['title']}_{geo_name}"

                    point = Point("demanda_evolucion_ccaa") \
                        .tag("title", title_with_geo) \
                        .field("value", valor_decimal) \
                        .time(value["datetime"], WritePrecision.S)
                    write_api.write(bucket=influxdb_bucket, record=point)

            print(f"Datos para el año {anno}, geo_id {geo_id}, {tipo_demanda[0]}/{tipo_demanda[1]} escritos en InfluxDB con éxito.")
    except Exception as e:
        print(f"Error procesando {tipo_demanda[0]}/{tipo_demanda[1]} geo_id {geo_id}: {e}")

