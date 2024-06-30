import paho.mqtt.client as mqtt
import pandas as pd
import time
from datetime import datetime, timedelta

# Configuración
broker = "172.21.0.3"  # Dirección del broker MQTT
port = 1883           # Puerto del broker MQTT
topic = "iborraberganza/co2"
username = "iborraberganzaco2"
password = "ib4.co2"

archivo_csv = '/home/azureuser/comunidades_acortado/Cataluna.csv'
df = pd.read_csv(archivo_csv)
df['Fecha'] = pd.to_datetime(df['Fecha'], format="%Y-%m-%d")
client = mqtt.Client()
client.username_pw_set(username, password)
client.connect(broker, port, 60)

def publish_messages():
    mensajes_acumulados = []
    intervalo_tiempo = timedelta(minutes=1)
    ultimo_tiempo_publicacion = datetime.now()
    max_size = 100
    contador = 0
    ultima_comunidad = None

    for index, row in df.iterrows():
        comunidad_actual = row['Comunidad_Autonoma']
        if comunidad_actual != ultima_comunidad and ultima_comunidad is not None:
            print(f"Comunidad Autónoma procesada: {ultima_comunidad}. Pausando 30 segundos...")
            client.disconnect()
            client.connect(broker, port, 60)
            print(f"Continuando con la comunidad autónoma: {comunidad_actual}")
        ultima_comunidad = comunidad_actual


        timestamp_datetime = row['Fecha']
        time_unix_ns = int(timestamp_datetime.timestamp() * 1e9)
        
        message = (f"{topic},Comunidad_Autonoma={row['Comunidad_Autonoma']},"
                   f"Sensor={row['Sensor']} "
                   f"Valor={row['Valor']} {time_unix_ns}")
        
        mensajes_acumulados.append(message)
        contador = contador +1
        
        if (datetime.now() - ultimo_tiempo_publicacion) >= intervalo_tiempo or len(mensajes_acumulados) >= max_size:
            mensaje_completo = '\n'.join(mensajes_acumulados)
            client.publish(topic, mensaje_completo)
            print(f"Publicado: {contador}")
            mensajes_acumulados = []
            ultimo_tiempo_publicacion = datetime.now()
            time.sleep(0.8)
            
    # Publicar cualquier mensaje acumulado restante
    if mensajes_acumulados:
        mensaje_completo = '\n'.join(mensajes_acumulados)
        client.publish(topic, mensaje_completo)
        print(f"Publicado (final): {contador}")

publish_messages()
print("Terminado")
client.disconnect()