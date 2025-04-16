import paho.mqtt.client as mqtt


Group_ID = 1
BROKER = "broker.hivemq.com"
PORT = 1883
TOPIC = f"machine_{Group_ID}/controller"


def on_connect(client, userdata, flags, rc):
    print(f"[CONNECTED] Return code: {rc}")
    client.subscribe(TOPIC)
    print(f"[SUBSCRIBED] to topic '{TOPIC}'")


def on_message(client, userdata, msg):
    print(f"[MESSAGE] {msg.topic}: {msg.payload.decode()}")


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)
client.loop_start()

welcome = """\
======== CONTROL CONSOLE ===========
========= TYPE 0 to turn OFF ==========
========= TYPE 1 to turn ON ===========
"""
print(welcome)

import time

try:
    while True:
        comand = input()
        if comand not in ("0", "1"):
            print("Command not allowed, try 1 or 0\n")
        else:
            client.publish(TOPIC, comand)
            print("[Message] sent")
except KeyboardInterrupt:
    print(" CLOSING...")
