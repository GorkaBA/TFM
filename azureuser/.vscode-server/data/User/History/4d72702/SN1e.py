import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import time

# Parámetros de InfluxDB
influxdb_url = "http://localhost:8086"
influxdb_token = "MH7gG-ayy-Oum10exnUj9eXLnMRXVSna2aSmRcDHTXt2E4iFZLE1Ozms9McDA5KHFYhpZw823A5Onbma2UNbbg=="
influxdb_org = "tfm"
influxdb_bucket = "test"

# Mapeo de geo_id a nombres de comunidades autónomas
geo_names = {
    1: "Ceuta",
    2: "Melilla",
    3: "Islas_Baleares",
    4: "Andalucia",
    5: "Aragon",
    6: "Cantabria",
    7: "Castilla_La_Mancha",
    8: "Castilla_y_Leon",
    9: "Cataluna",
    10: "Pais_Vasco",
    11: "Principado_de_Asturias",
    12: "Canarias",
    13: "Comunidad_de_Madrid",
    14: "Comunidad_Foral_de_Navarra",
    15: "Comunidad_Valenciana",
    16: "Extremadura",
    17: "Galicia",
    20: "La_Rioja",
    21: "Region_de_Murcia"
}

# Conectar a InfluxDB
client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
write_api = client.write_api(write_options=SYNCHRONOUS)



def demanda(anno):
    print(anno)
    api_url = f"https://apidatos.ree.es/es/datos/demanda/evolucion?end_date={anno}-12-31T23:59&time_trunc=day&start_date={anno}-12-01T04:00"

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

                point = Point("demanda_evolucion") \
                    .tag("title", item["attributes"]["title"]) \
                    .field("value_float", valor_decimal) \
                    .time(value["datetime"], WritePrecision.S)
                write_api.write(bucket=influxdb_bucket, record=point)

        print(f"Datos para el año {anno} escritos en InfluxDB con éxito.")

def comunidades(anno):
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
        demanda_ccaa(anno, geo_name, geo_id)
        
def demanda_ccaa(anno, geo_name, geo_id):
    # Parámetros de la API
        api_url = f"https://apidatos.ree.es/es/datos/demanda/evolucion?end_date={anno}-12-31T23:59&time_trunc=month&start_date={anno}-01-01T04:00&geo_trunc=electric_system&geo_limit=ccaa&geo_ids={geo_id}"

        # Obtener datos de la API
        response = requests.get(api_url)
        data = response.json()

        # Verificar si la solicitud fue exitosa
        if response.status_code != 200:
            print(f"Error al obtener datos de la API para el año {anno}, geo_id {geo_id}:", response.status_code)
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

                    # Agregar el nombre de la comunidad al título
                    title_with_geo = f"{item['attributes']['title']}_{geo_name}"

                    point = Point("demanda_evolucion_ccaa") \
                        .tag("title", title_with_geo) \
                        .field("value", valor_decimal) \
                        .time(value["datetime"], WritePrecision.S)
                    write_api.write(bucket=influxdb_bucket, record=point)

            print(f"Datos para el año {anno}, geo_id {geo_id} escritos en InfluxDB con éxito.")


for anno in range(2023, 2024):
    #comunidades(anno)
    demanda(anno)
    

# Cerrar conexión
client.close()