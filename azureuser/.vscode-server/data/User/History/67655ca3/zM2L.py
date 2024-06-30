import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import time
import datetime
from datetime import timedelta

# Parámetros de InfluxDB
influxdb_url = "http://localhost:8086"
influxdb_token = "MH7gG-ayy-Oum10exnUj9eXLnMRXVSna2aSmRcDHTXt2E4iFZLE1Ozms9McDA5KHFYhpZw823A5Onbma2UNbbg=="
influxdb_org = "tfm"
influxdb_bucket = "test2"

#Diccionario de lo que se va a generar con sus atributos
nombres = [
    ("balance", "balance-electrico","day",True),
    ("demanda", "evolucion", "day", True),
    ("demanda", "perdidas-transporte", "month", False),4
    ("demanda","demanda-tiempo-real", "hour", False),
    #("demanda", "demanda-maxima-diaria", "day", False),
    ("generacion", "estructura-generacion", "month", True),
    ("generacion", "evolucion-renovable-no-renovable", "month", True),
    ("generacion", "estructura-renovables", "month", True),
    ("generacion", "estructura-generacion-emisiones-asociadas", "month", True),
    ("generacion", "evolucion-estructura-generacion-emisiones-asociadas", "month", True),
    ("generacion", "no-renovables-detalle-emisiones-CO2", "month", False),
    ("generacion", "maxima-renovable", "month", False),
    ("generacion", "potencia-instalada", "month", True),
    ("generacion", "maxima-renovable-historico", "month", False),
    ("generacion", "maxima-sin-emisiones-historico", "month", False),
    ("intercambios", "francia-frontera", "day", False),
    ("intercambios", "portugal-frontera", "day", False),
    ("intercambios", "marruecos-frontera", "day", False),
    ("intercambios", "andorra-frontera", "day", False),
    ("intercambios", "lineas-francia", "day", False),
    ("intercambios", "lineas-portugal", "day", False),
    ("intercambios", "lineas-marruecos", "day", False),
    ("intercambios", "lineas-andorra", "day", False),
    ("intercambios", "francia-frontera-programado", "day", False),
    ("intercambios", "portugal-frontera-programado", "day", False),
    ("intercambios", "marruecos-frontera-programado", "day", False),
    ("intercambios", "andorra-frontera-programado", "day", False),
    ("intercambios", "enlace-baleares", "day", False),
    ("intercambios", "frontera-fisicos", "day", False),
    ("intercambios", "todas-fronteras-fisicos", "day", False),
    ("intercambios", "frontera-programados", "day", False),
    ("intercambios", "todas-fronteras-programados", "day", False),
    ("mercados", "componentes-precio-energia-cierre-desglose", "month", False),
    ("mercados", "componentes-precio", "month", False),
    ("mercados", "energia-gestionada-servicios-ajuste", "month", False),
    ("mercados", "energia-restricciones", "month", False),
    ("mercados", "precios-restricciones", "month", False),
    ("mercados", "reserva-potencia-adicional", "month", False),
    ("mercados", "banda-regulacion-secundaria", "month", False),
    ("mercados", "energia-precios-regulacion-secundaria", "month", False),
    ("mercados", "energia-precios-regulacion-terciaria", "month", False),
    ("mercados", "energia-precios-gestion-desvios", "month", False),
    ("mercados", "coste-servicios-ajuste", "month", False),
    ("mercados", "volumen-energia-servicios-ajuste-variacion", "month", False),
    ("mercados", "precios-mercados-tiempo-real", "hour", False),
    ("mercados", "energia-precios-ponderados-gestion-desvios-before", "month", False),
    ("mercados", "energia-precios-ponderados-gestion-desvios", "month", False),
    ("mercados", "energia-precios-ponderados-gestion-desvios-after", "month", False)
]

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

def demanda_half_month(anno, tipo_demanda):
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


def demanda(anno, tipo_demanda):

    api_url = f"https://apidatos.ree.es/es/datos/{tipo_demanda[0]}/{tipo_demanda[1]}?end_date={anno}-12-31T23:59&time_trunc={tipo_demanda[2]}&start_date={anno}-01-01T04:00"
    print(api_url)
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
                    .field("value", valor_decimal) \
                    .time(value["datetime"], WritePrecision.S)
                write_api.write(bucket=influxdb_bucket, record=point)

        print(f"Datos para el año {anno} {tipo_demanda[0]}/{tipo_demanda[1]} escritos en InfluxDB con éxito.")

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
        demanda_ccaa(anno, geo_name, geo_id,tipo_demanda)
        
def demanda_ccaa(anno, geo_name, geo_id, tipo_demanda):
    # Parámetros de la API
        # if tipo_demanda =="perdidas-transporte":
        #     return
        
        # if tipo_demanda == "demanda-maxima-diaria":
        #     time_trunc = "day"
        # else:
        #     time_trunc = "month"
        

        api_url = f"https://apidatos.ree.es/es/datos/{tipo_demanda[0]}/{tipo_demanda[1]}?end_date={anno}-12-31T23:59&time_trunc=month&start_date={anno}-01-01T04:00&geo_trunc=electric_system&geo_limit=ccaa&geo_ids={geo_id}"

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

            print(f"Datos para el año {anno}, geo_id {geo_id}, {tipo_demanda[0]}/{tipo_demanda[1]} escritos en InfluxDB con éxito.")


for anno in range(2022, 2023):
    # tipo_demanda = {"evolucion", "perdidas-transporte", "demanda-maxima-diaria"}
    # tipo_generacion = {"estructura-generacion", "evolucion-renovable-no-renovable", "estructura-renovables", "estructura-generacion-emisiones-asociadas","evolucion-estructura-generacion-emisiones-asociadas",
    # "no-renovables-detalle-emisiones-CO2", "maxima-renovable", "potencia-instalada", "maxima-renovable-historico", "maxima-sin-emisiones-historico"}
    
    for elemento in nombres:
        if elemento[2] == "hour":
            demanda_hour(anno, elemento)
        else:
            demanda(anno, elemento)

        if elemento[3]:
            comunidades(anno, elemento)
             


  
    

# Cerrar conexión
client.close()