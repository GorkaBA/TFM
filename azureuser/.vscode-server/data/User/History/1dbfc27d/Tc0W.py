# Diccionario de lo que se va a generar con sus atributos
influxdb_url = "http://localhost:8086"
influxdb_token = "MH7gG-ayy-Oum10exnUj9eXLnMRXVSna2aSmRcDHTXt2E4iFZLE1Ozms9McDA5KHFYhpZw823A5Onbma2UNbbg=="
influxdb_org = "tfm"
influxdb_bucket = "raw_api_data"
measurement = "evolucion"

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