import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta
from config import nombres, geo_names, influxdb_token, influxdb_bucket, influxdb_org, influxdb_url
from balance_carga import consulta_balance_electrico, balance_comunidades
# Parámetros de InfluxDB
# influxdb_url = "http://localhost:8086"
# influxdb_token = "MH7gG-ayy-Oum10exnUj9eXLnMRXVSna2aSmRcDHTXt2E4iFZLE1Ozms9McDA5KHFYhpZw823A5Onbma2UNbbg=="
# influxdb_org = "tfm"
# influxdb_bucket = "test2"

# Conectar a InfluxDB
client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Lista para almacenar los nombres de los elementos que fallan
fallos = []

def demanda_hour(anno, tipo_demanda):
    try:
        for mes in range(1, 13):
            for mitad in range(2):
                if mitad == 0:
                    start_date = datetime(anno, mes, 1)
                    if mes == 2:
                        end_date = start_date + timedelta(days=14) - timedelta(seconds=1)
                    else:
                        end_date = start_date + timedelta(days=15) - timedelta(seconds=1)
                else:
                    start_date = datetime(anno, mes, 15 if mes != 2 else 14) + timedelta(seconds=1)
                    if mes == 2:
                        if anno % 4 == 0 and (anno % 100 != 0 or anno % 400 == 0):
                            end_date = datetime(anno, mes, 29) - timedelta(seconds=1)
                        else:
                            end_date = datetime(anno, mes, 28) - timedelta(seconds=1)
                    else:
                        if mes in [1, 3, 5, 7, 8, 10, 12]:
                            end_date = datetime(anno, mes, 31) - timedelta(seconds=1)
                        else:
                            end_date = datetime(anno, mes, 30) - timedelta(seconds=1)

                api_url = f"https://apidatos.ree.es/es/datos/{tipo_demanda[0]}/{tipo_demanda[1]}?end_date={end_date.strftime('%Y-%m-%dT%H:%M:%S')}&time_trunc={tipo_demanda[2]}&start_date={start_date.strftime('%Y-%m-%dT%H:%M:%S')}"
                print(api_url)

                # Obtener datos de la API
                response = requests.get(api_url)
                data = response.json()

                # Verificar si la solicitud fue exitosa
                if response.status_code != 200:
                    print(f"Error al obtener datos de la API para el intervalo {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')} del año {anno}:", response.status_code)
                    print("Mensaje:", response.text)
                    fallos.append(f"{tipo_demanda[0]}/{tipo_demanda[1]}: {response.status_code}")
                    continue
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

                            point = Point("demanda_evolucion") \
                                .tag("title", item["attributes"]["title"]) \
                                .field("value", valor_decimal) \
                                .time(value["datetime"], WritePrecision.S)
                            write_api.write(bucket=influxdb_bucket, record=point)

                    print(f"Datos para el intervalo {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')} del año {anno} {tipo_demanda[0]}/{tipo_demanda[1]} escritos en InfluxDB con éxito.")
    except Exception as e:
        print(f"Error procesando {tipo_demanda[0]}/{tipo_demanda[1]}: {e}")
        fallos.append(f"{tipo_demanda[0]}/{tipo_demanda[1]}: {e}")

def demanda(anno, tipo_demanda):
    try:
        api_url = f"https://apidatos.ree.es/es/datos/{tipo_demanda[0]}/{tipo_demanda[1]}?end_date={anno}-12-31T23:59&time_trunc={tipo_demanda[2]}&start_date={anno}-01-01T04:00"

        print(api_url)
        # Obtener datos de la API
        response = requests.get(api_url)
        data = response.json()

        # Verificar si la solicitud fue exitosa
        if response.status_code != 200:
            print(f"Error al obtener datos de la API para el año {anno}:", response.status_code)
            print("Mensaje:", response.text)
            fallos.append(f"{tipo_demanda[0]}/{tipo_demanda[1]}: {response.status_code}")
            return
        else:
            # Verificar estructura de datos
            if 'included' not in data:
                print(f"Estructura de datos inesperada para el año {anno}: 'included' no encontrado")
                print(data)
                fallos.append(f"{tipo_demanda[0]}/{tipo_demanda[1]}: 'included' no encontrado")
                return

            for item in data['included']:
                if 'attributes' not in item or 'values' not in item['attributes']:
                    print(f"Estructura de datos inesperada en item: {item}")
                    fallos.append(f"{tipo_demanda[0]}/{tipo_demanda[1]}: Estructura de datos inesperada en item")
                    continue

                for value in item['attributes']['values']:
                    # Convertir el valor a float y formatear a 3 decimales
                    try:
                        valor_decimal = float("{:.3f}".format(float(value["value"])))
                    except ValueError as e:
                        print(f"Error al convertir el valor: {value['value']} a float: {e}")
                        continue

                    point = Point("demanda_evolucion") \
                        .tag("title", item["attributes"]["title"]) \
                        .field("value", valor_decimal) \
                        .time(value["datetime"], WritePrecision.S)
                    write_api.write(bucket=influxdb_bucket, record=point)

            print(f"Datos para el año {anno} {tipo_demanda[0]}/{tipo_demanda[1]} escritos en InfluxDB con éxito.")
    except Exception as e:
        print(f"Error procesando {tipo_demanda[0]}/{tipo_demanda[1]}: {e}")
        fallos.append(f"{tipo_demanda[0]}/{tipo_demanda[1]}: {e}")

def comunidades(anno, tipo_demanda):
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
            print(f"Error al obtener datos de la API para el año {anno}, geo_id {geo_id}:", response.status_code)
            print("Mensaje:", response.text)
            fallos.append(f"{tipo_demanda[0]}/{tipo_demanda[1]} geo_id {geo_id}: {response.status_code}")
            return
        else:
            # Verificar estructura de datos
            if 'included' not in data:
                print(f"Estructura de datos inesperada para el año {anno}, geo_id {geo_id}: 'included' no encontrado")
                print(data)
                fallos.append(f"{tipo_demanda[0]}/{tipo_demanda[1]} geo_id {geo_id}: 'included' no encontrado")
                return

            for item in data['included']:
                if 'attributes' not in item or 'values' not in item['attributes']:
                    print(f"Estructura de datos inesperada en item: {item}")
                    fallos.append(f"{tipo_demanda[0]}/{tipo_demanda[1]} geo_id {geo_id}: Estructura de datos inesperada en item")
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
        fallos.append(f"{tipo_demanda[0]}/{tipo_demanda[1]} geo_id {geo_id}: {e}")

for anno in range(2018, 2019):
    print(anno)
    # for elemento in nombres:
    #     if elemento[2] == "hour":
    #         demanda_hour(anno, elemento)
    #     else:
    #         demanda(anno, elemento)

    #     if elemento[3]:
    #         comunidades(anno, elemento)
    consulta_balance_electrico(anno)

# Imprimir los elementos que fallaron
print("Elementos que fallaron:")
for fallo in fallos:
    print(fallo)

