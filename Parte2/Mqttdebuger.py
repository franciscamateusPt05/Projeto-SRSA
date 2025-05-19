import paho.mqtt.client as mqtt
from datetime import datetime
GroupID=''
topic = f'v3/{GroupID}@ttn/devices/#'
testtopic = "v3/100@ttn/devices/M1/up"
broker = '10.6.1.9'
port = 1883

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe(topic)

def on_message(client, userdata, message):
    print(f" {datetime.now()} {message.topic}  {message.payload.decode()}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(broker, port)
client.loop_forever()
