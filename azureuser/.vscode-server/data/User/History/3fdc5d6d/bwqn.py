import paho.mqtt.client as mqtt
import pandas as pd
from datetime import datetime
import time
import glob

# Configuración
broker = "192.168.32.1"  # Dirección del broker MQTT
port = 1883           # Puerto del broker MQTT
topic = "iborraberganza/co2"
username = "iborraberganzaco2"
password = "ib4.co"
# cert_path = "/home/ikeri/configurations/docker_nanomq/etc/ssl_tls_certs/cert.pem"


# Ruta de los datos
carpeta_principal = '/home/azureuser/data'
dataframes = []

# Buscar todos los archivos CSV en la carpeta principal
archivos_csv = glob.glob(f"{carpeta_principal}/**/*.csv", recursive=True)

# Iterar sobre cada archivo CSV encontrado
for archivo_csv in archivos_csv:
    # Cargar el archivo CSV como DataFrame y añadirlo a la lista
    df = pd.read_csv(archivo_csv)
    dataframes.append(df)

# Concatenar todos los DataFrames en uno solo
df_final = pd.concat(dataframes, ignore_index=True)

# Convertir la columna 'Time' a datetime
df_final['Time'] = pd.to_datetime(df_final['Time'], format="%Y-%m-%d %H:%M:%S.%f")

# Crear una instancia del cliente MQTT
client = mqtt.Client()

# Establecer las credenciales de usuario
client.username_pw_set(username, password)

# Configurar TLS/SSL si es necesario
# client.tls_set(cert_path, tls_version=mqtt.ssl.PROTOCOL_TLS)

# Conectar al broker
client.connect(broker, port, 60)

def publish_messages():
    for index, row in df_final.iterrows():
        # Asegurarse de que la columna Time se procesa correctamente
        timestamp_datetime = row['Time']
        # Obtener el tiempo en formato Unix epoch en nanosegundos
        time_unix_ns = int(timestamp_datetime.timestamp() * 1e9)
        # Crear el payload en formato InfluxDB line protocol, incluyendo el campo time
        message = (f"test "
                   f"Vela1_Mx={row['Vela1_Mx']},Vela1_Mz={row['Vela1_Mz']},"
                   f"Vela1_Mresultant={row['Vela1_Mresultant']},Vela2_Mx={row['Vela2_Mx']},"
                   f"Vela2_Mz={row['Vela2_Mz']},Vela2_Mresultant={row['Vela2_Mresultant']} "
                   f"{time_unix_ns}")
        # Publicar el mensaje en el tema MQTT
        client.publish(topic, message)
        print(f"Publicado: {message}")
        time.sleep(1)  # Esperar un segundo entre publicaciones

# Iniciar la publicación
publish_messages()

# Mantener el cliente MQTT en funcionamiento
client.loop_forever()