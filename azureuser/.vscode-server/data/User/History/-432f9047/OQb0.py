import paho.mqtt.client as mqtt
import pandas as pd
from datetime import datetime, timedelta

# Configuración de MQTT
broker = "172.22.0.2"  # Dirección del broker MQTT
port = 1883           # Puerto del broker MQTT
topic = "iborraberganza/co2"
username = "iborraberganzaco2"
password = "ib4.co2"

# Ruta del archivo CSV
archivo_csv = '/home/azureuser/archivo.csv'

# Leer el archivo CSV
df = pd.read_csv(archivo_csv)

# Convertir la columna 'Fecha' a datetime
df['Fecha'] = pd.to_datetime(df['Fecha'], format="%Y-%m-%d")

# Crear una instancia del cliente MQTT
client = mqtt.Client()

# Establecer las credenciales de usuario
client.username_pw_set(username, password)

# Conectar al broker
client.connect(broker, port, 60)

def publish_messages():
    mensajes_acumulados = []
    intervalo_tiempo = timedelta(minutes=1)
    ultimo_tiempo_publicacion = datetime.now()
    max_size = 100  # Tamaño máximo del array de mensajes
    contador = 0

    for index, row in df.iterrows():
        # Asegurarse de que la columna Fecha se procesa correctamente
        timestamp_datetime = row['Fecha']
        # Obtener el tiempo en formato Unix epoch en segundos
        time_unix_s = int(timestamp_datetime.timestamp())
        message = f"{topic} {row['Comunidad Autónoma']},Sensor={row['Sensor']} Valor={row['Valor']} {time_unix_s}"
        mensajes_acumulados.append(message)
        contador += 1

        # Verificar si se debe publicar el array de mensajes
        if (datetime.now() - ultimo_tiempo_publicacion) >= intervalo_tiempo or len(mensajes_acumulados) >= max_size:
            # Unir las cadenas de texto en una sola cadena
            mensaje_completo = '\n'.join(mensajes_acumulados)
            client.publish(topic, mensaje_completo)
            print(f"Publicado (grupo): {contador}")
            mensajes_acumulados = []
            ultimo_tiempo_publicacion = datetime.now()

    # Publicar cualquier mensaje acumulado restante
    if mensajes_acumulados:
        mensaje_completo = '\n'.join(mensajes_acumulados)
        client.publish(topic, mensaje_completo)
        print(f"Publicado (final): {contador}")

# Iniciar la publicación
publish_messages()

# Mantener el cliente MQTT en funcionamiento
client.loop_forever()
