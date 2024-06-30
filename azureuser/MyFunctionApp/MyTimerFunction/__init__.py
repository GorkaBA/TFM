import logging
import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta, timezone
import sys
import os
import azure.functions as func

# Añadir la ruta del directorio 'carga_de_datos' al path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'carga_de_datos')))

# Diccionario de lo que se va a generar con sus atributos
influxdb_url = "http://localhost:8086"
influxdb_token = "MH7gG-ayy-Oum10exnUj9eXLnMRXVSna2aSmRcDHTXt2E4iFZLE1Ozms9McDA5KHFYhpZw823A5Onbma2UNbbg=="
influxdb_org = "tfm"
influxdb_bucket = "raw_api_data"

nombres = [
    ("demanda", "evolucion", "day", True),
    ("demanda", "perdidas-transporte", "month", False),
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
   # ("mercados", "energia-precios-gestion-desvios", "month", False),
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


def consulta_balance_electrico(anno):
    try:
        api_url = f"https://apidatos.ree.es/es/datos/balance/balance-electrico?end_date={anno}-12-31T23:59&time_trunc=day&start_date={anno}-01-01T00:00"
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
                            porcentaje_decimal = float("{:.3f}".format(float(value["percentage"])))
                        except ValueError as e:
                            print(f"Error al convertir el valor: {value['value']} a float: {e}")
                            continue

                        point = Point("balance_electrico") \
                            .tag("Caracteristicas", item["type"]) \
                            .tag("Categoria", "balance") \
                            .tag("Tema", "balance_electrico_general") \
                            .tag("Tipo", item["attributes"]["title"]) \
                            .field("Valor", valor_decimal) \
                            .field("Porcentaje", porcentaje_decimal) \
                            .time(value["datetime"], WritePrecision.S)
                        write_api.write(bucket=influxdb_bucket, record=point)

            print(f"Datos de balance eléctrico escritos en InfluxDB con éxito.")
    except Exception as e:
        print(f"Error procesando la consulta de balance eléctrico: {e}")

def balance_comunidades(anno):
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
        balance_ccaa(anno, geo_name, geo_id)


def balance_ccaa(anno, geo_name, geo_id):
    try:
        # Parámetros de la API
        api_url = f"https://apidatos.ree.es/es/datos/balance/balance-electrico?end_date={anno}-12-31T23:59&time_trunc=month&start_date={anno}-01-01T00:00&geo_trunc=electric_system&geo_limit=ccaa&geo_ids={geo_id}"
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
                            porcentaje_decimal = float("{:.3f}".format(float(value["percentage"])))
                        except ValueError as e:
                            print(f"Error al convertir el valor: {value['value']} a float: {e}")
                            continue
                        point = Point("balance_electrico") \
                            .tag("Caracteristicas", item["type"]) \
                            .tag("Categoria", "balance") \
                            .tag("Tema", "balance_electrico_ccaa") \
                            .tag("Comunidad", geo_name) \
                            .tag("Tipo", item["attributes"]["title"]) \
                            .field("Valor", valor_decimal) \
                            .field("Porcentaje", porcentaje_decimal) \
                            .time(value["datetime"], WritePrecision.S)
                        write_api.write(bucket=influxdb_bucket, record=point)

            print(f"Datos de balance eléctrico escritos en InfluxDB con éxito.")
    except Exception as e:
        print(f"Error procesando la consulta de balance eléctrico: {e}")

def demanda_hour(tipo_demanda):
    try:
        now = datetime.now()
        anno = now.year
        mes = now.month

        for mitad in range(2):
            if mitad == 0:
                start_date = datetime(anno, mes, 1)
                end_date = start_date + timedelta(days=14) - timedelta(seconds=1)
            else:
                start_date = datetime(anno, mes, 15) + timedelta(seconds=1)
                end_date = now if now.month == mes else (start_date.replace(month=mes) + timedelta(days=30)) - timedelta(seconds=1)

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

                        point = Point(tipo_demanda[1]) \
                            .tag("Categoria", tipo_demanda[0]) \
                            .tag("Tema", f"{tipo_demanda[1]}_general") \
                            .tag("Tipo", item["attributes"]["title"]) \
                            .field("Valor", valor_decimal) \
                            .time(value["datetime"], WritePrecision.S)
                        write_api.write(bucket=influxdb_bucket, record=point)

                print(f"Datos para el intervalo {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')} del año {anno} {tipo_demanda[0]}/{tipo_demanda[1]} escritos en InfluxDB con éxito.")
    except Exception as e:
        print(f"Error procesando {tipo_demanda[0]}/{tipo_demanda[1]}: {e}")

def demanda(tipo_demanda):
    try:
        now = datetime.now()
        anno = now.year
        mes = now.month

        api_url = f"https://apidatos.ree.es/es/datos/{tipo_demanda[0]}/{tipo_demanda[1]}?end_date={anno}-{mes:02d}-{now.day:02d}T23:59&time_trunc={tipo_demanda[2]}&start_date={anno}-{mes:02d}-01T04:00"

        print(api_url)
        # Obtener datos de la API
        response = requests.get(api_url)
        data = response.json()

        # Verificar si la solicitud fue exitosa
        if response.status_code != 200:
            print(f"Error al obtener datos de la API para el año {anno}, mes {mes}:", response.status_code)
            print("Mensaje:", response.text)
            return
        else:
            # Verificar estructura de datos
            if 'included' not in data:
                print(f"Estructura de datos inesperada para el año {anno}, mes {mes}: 'included' no encontrado")
                return

            for item in data['included']:
                if 'attributes' not in item or 'values' not in item['attributes']:
                    print(f"Estructura de datos inesperada en item:")
                    continue

                for value in item['attributes']['values']:
                    # Convertir el valor a float y formatear a 3 decimales
                    try:
                        valor_decimal = float("{:.3f}".format(float(value["value"])))
                    except ValueError as e:
                        print(f"Error al convertir el valor: {value['value']} a float: {e}")
                        continue

                    point = Point(tipo_demanda[1]) \
                        .tag("Categoria", tipo_demanda[0]) \
                        .tag("Tema", f"{tipo_demanda[1]}_general") \
                        .tag("Tipo", item["attributes"]["title"]) \
                        .field("Valor", valor_decimal) \
                        .time(value["datetime"], WritePrecision.S)
                    write_api.write(bucket=influxdb_bucket, record=point)

            print(f"Datos para el año {anno}, mes {mes} {tipo_demanda[0]}/{tipo_demanda[1]} escritos en InfluxDB con éxito.")
    except Exception as e:
        print(f"Error procesando {tipo_demanda[0]}/{tipo_demanda[1]}: {e}")

def comunidades(tipo_demanda):
    now = datetime.now()
    anno = now.year
    mes = now.month

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
        demanda_ccaa(anno, mes, now.day, geo_name, geo_id, tipo_demanda)

def demanda_ccaa(anno, mes, day, geo_name, geo_id, tipo_demanda):
    try:
        # Parámetros de la API
        api_url = f"https://apidatos.ree.es/es/datos/{tipo_demanda[0]}/{tipo_demanda[1]}?end_date={anno}-{mes:02d}-{day:02d}T23:59&time_trunc=month&start_date={anno}-{mes:02d}-01T04:00&geo_trunc=electric_system&geo_limit=ccaa&geo_ids={geo_id}"

        # Obtener datos de la API
        response = requests.get(api_url)
        data = response.json()

        # Verificar si la solicitud fue exitosa
        if response.status_code != 200:
            print(f"Error al obtener datos de la API para el año {anno}, mes {mes}, geo_id {geo_id}:", response.status_code)
            print("Mensaje:", response.text)
            return
        else:
            # Verificar estructura de datos
            if 'included' not in data:
                print(f"Estructura de datos inesperada para el año {anno}, mes {mes}, geo_id {geo_id}: 'included' no encontrado")
                return

            for item in data['included']:
                if 'attributes' not in item or 'values' not in item['attributes']:
                    print(f"Estructura de datos inesperada en item:")
                    continue

                for value in item['attributes']['values']:
                    # Convertir el valor a float y formatear a 3 decimales
                    try:
                        valor_decimal = float("{:.3f}".format(float(value["value"])))
                    except ValueError as e:
                        print(f"Error al convertir el valor: {value['value']} a float: {e}")
                        continue

                    # Agregar el nombre de la comunidad al título

                    point = Point(f"{tipo_demanda[1]}_ccaa") \
                        .tag("Comunidad", geo_name) \
                        .tag("Categoria", tipo_demanda[0]) \
                        .tag("Tema", f"{tipo_demanda[1]}_ccaa") \
                        .tag("Tipo", item["attributes"]["title"]) \
                        .field("Valor", valor_decimal) \
                        .time(value["datetime"], WritePrecision.S)
                    write_api.write(bucket=influxdb_bucket, record=point)

            print(f"Datos para el año {anno}, mes {mes}, geo_id {geo_id}, {tipo_demanda[0]}/{tipo_demanda[1]} escritos en InfluxDB con éxito.")
    except Exception as e:
        print(f"Error procesando {tipo_demanda[0]}/{tipo_demanda[1]} geo_id {geo_id}: {e}")

# Función principal para Azure Functions

@app.timer_trigger(schedule="0 0 15 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    
    # Obtener el tipo de demanda
    for elemento in nombres:
        if elemento[2] == "hour":
            demanda_hour(elemento)
        else:
            demanda(elemento)
        if elemento[3] == True:
            comunidades(elemento)

    consulta_balance_electrico(datetime.now().year)
    consulta_balance_electrico(datetime.now().year)
