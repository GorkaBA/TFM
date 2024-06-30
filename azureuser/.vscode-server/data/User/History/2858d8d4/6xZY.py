import paho.mqtt.client as mqtt
import pandas as pd
import time
from datetime import datetime, timedelta

# Configuración
broker = "172.18.0.3"  # Dirección del broker MQTT
port = 1883           # Puerto del broker MQTT
username = "iborraberganzaco2"
password = "ib4.co2"
topic = "iborraberganza/co2"

# Ruta del archivo CSV
archivo_csv = '/home/azureuser/emisiones_gei_recortadas.xlsx'

# Leer el archivo CSV
df = pd.read_csv(archivo_csv)

# Convertir la columna 'Fecha' a datetime
df['Fecha'] = pd.to_datetime(df['Fecha'], format="%Y-%m-%d")

def publish_messages(messages):
    # Crear una instancia del cliente MQTT
    client = mqtt.Client()
    # Establecer las credenciales de usuario
    client.username_pw_set(username, password)
    # Conectar al broker
    client.connect(broker, port, 60)
    # Publicar mensajes
    client.publish(topic, messages)
    # Desconectar el cliente MQTT
    client.disconnect()

def process_community(df):
    messages = []
    max_size = 100  # Ajusta el tamaño máximo según tus necesidades
    contador = 0

    for index, row in df.iterrows():
        # Asegurarse de que la columna Fecha se procesa correctamente
        timestamp_datetime = row['Fecha']
        # Obtener el tiempo en formato Unix epoch en nanosegundos
        time_unix_ns = int(timestamp_datetime.timestamp() * 1e9)
        
        # Crear el mensaje en formato InfluxDB line protocol
        message = (f"Comunidad_Autonoma={row['Comunidad_Autonoma']},"
                   f"Sensor={row['Sensor']} "
                   f"Valor={row['Valor']} {time_unix_ns}")
        
        messages.append(message)
        contador += 1
        
        if len(messages) >= max_size:
            publish_messages('\n'.join(messages))
            print(f"Publicado: {contador}")
            messages = []
            time.sleep(0.5)

    # Publicar cualquier mensaje acumulado restante
    if messages:
        publish_messages('\n'.join(messages))
        print(f"Publicado (final): {contador}")

def main():
    last_community = None

    for community, df_community in df.groupby('Comunidad_Autonoma'):
        print(f"Procesando comunidad autónoma: {community}")
        if last_community is not None:
            print(f"Publicando mensajes de la comunidad autónoma anterior: {last_community}")
            process_community(df_last_community)
        df_last_community = df_community.copy()
        last_community = community

    if last_community is not None:
        print(f"Publicando mensajes de la última comunidad autónoma: {last_community}")
        process_community(df_last_community)

if __name__ == "__main__":
    main()
    print("Terminado")