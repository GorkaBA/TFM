import paho.mqtt.client as mqtt
import pandas as pd
import time

# Configuración
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
    for index, row in df.iterrows():
        # Asegurarse de que la columna Fecha se procesa correctamente
        timestamp_datetime = row['Fecha']
        # Obtener el tiempo en formato Unix epoch en segundos
        time_unix_s = int(timestamp_datetime.timestamp())
        # Crear el payload en formato InfluxDB line protocol, incluyendo el campo time
        message = f"co2_sensors {row['Comunidad Autónoma']},Sensor={row['Sensor']} Valor={row['Valor']} {time_unix_s}"
        # Publicar el mensaje en el tema MQTT
        client.publish(topic, message)
        print(f"Publicado: {message}")
        time.sleep(1)  # Esperar un segundo entre publicaciones

# Iniciar la publicación
publish_messages()

# Mantener el cliente MQTT en funcionamiento
client.loop_forever()
