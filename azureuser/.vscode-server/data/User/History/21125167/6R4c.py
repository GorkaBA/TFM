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

# Ruta del archivo CSV
archivo_csv = '/home/azureuser/comunidades_acortado/Aragon.csv'

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
    max_size = 100  # Ajusta el tamaño máximo según tus necesidades
    contador = 0
    ultima_comunidad = None

    for index, row in df.iterrows():
        # Obtener la comunidad autónoma actual
        comunidad_actual = row['Comunidad_Autonoma']
        # Comprobar si hemos cambiado de comunidad autónoma
        if comunidad_actual != ultima_comunidad and ultima_comunidad is not None:
            print(f"Comunidad Autónoma procesada: {ultima_comunidad}. Pausando 30 segundos...")
            # time.sleep(8)  # Pausa de 30 segundos
            client.disconnect()
            client.connect(broker, port, 60)
            print(f"Continuando con la comunidad autónoma: {comunidad_actual}")
        # Actualizar la última comunidad autónoma procesada
        ultima_comunidad = comunidad_actual


        # Asegurarse de que la columna Fecha se procesa correctamente
        timestamp_datetime = row['Fecha']
        # Obtener el tiempo en formato Unix epoch en nanosegundos
        time_unix_ns = int(timestamp_datetime.timestamp() * 1e9)
        
        # Crear el mensaje en formato InfluxDB line protocol
        message = (f"{topic},Comunidad_Autonoma={row['Comunidad_Autonoma']},"
                   f"Sensor={row['Sensor']} "
                   f"Valor={row['Valor']} {time_unix_ns}")
        
        mensajes_acumulados.append(message)
        contador = contador +1
        
        # Verificar si se debe publicar el array de mensajes
        if (datetime.now() - ultimo_tiempo_publicacion) >= intervalo_tiempo or len(mensajes_acumulados) >= max_size:
            mensaje_completo = '\n'.join(mensajes_acumulados)
            client.publish(topic, mensaje_completo)
            # print(f"Publicado: {mensaje_completo}")
            print(f"Publicado: {contador}")
            mensajes_acumulados = []
            ultimo_tiempo_publicacion = datetime.now()
            time.sleep(0.8)
            
    # Publicar cualquier mensaje acumulado restante
    if mensajes_acumulados:
        mensaje_completo = '\n'.join(mensajes_acumulados)
        client.publish(topic, mensaje_completo)
        # print(f"Publicado (final): {mensaje_completo}")
        print(f"Publicado (final): {contador}")

# Iniciar la publicación
publish_messages()
print("Terminado")
# Mantener el cliente MQTT en funcionamiento
client.disconnect()
#client.loop_forever()