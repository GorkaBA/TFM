import paho.mqtt.client as mqtt

broker = "172.20.0.2"
port = 1883
topic = "test/topic"

client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    print(f"Conectado al broker MQTT con código de resultado {rc}")

def on_publish(client, userdata, mid):
    print(f"Mensaje {mid} publicado.")

client.on_connect = on_connect
client.on_publish = on_publish

client.connect(broker, port, 60)
client.loop_start()

message = "Hello, NanoMQ!"
result = client.publish(topic, message)
print(f"Resultado de publicación: {result}")

client.loop_stop()
client.disconnect()
