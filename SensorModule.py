import paho.mqtt.client as mqtt
import random
import time
import os

Group_ID = 11
# === RANGES ===
temp = [10, 200]
oil = [0, 8]
rpm = [0, 4000]

# === TOPICS ===
topic_temp = f"machine_{Group_ID}/coolant"
topic_oil = f"machine_{Group_ID}/pressure"
topic_rpm = f"machine_{Group_ID}/rpm"

topic_controller = f"machine_{Group_ID}/controller"

will_message = "SENSORS DISCONNECTED UNEXPECTEDLY"

BROKER = "10.6.1.71"
PORT = 1883

active = False


# === CALLBACKS ===
def on_connect(client, userdata, flags, rc):
    global active
    print("[CONNECTED] to broker with code:", rc)
    client.publish(topic_controller, "Bulldozer ONLINE")
    active = True


def on_message(client, userdata, msg):
    print(f"[RECEIVED] {msg.topic}: {msg.payload.decode()}")


# === CLIENT ===
client = mqtt.Client()
client.will_set(topic_controller, payload=will_message, qos=1, retain=False)
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT)
client.loop_start()

try:

    while not active:
        time.sleep(0.1)
    while active:
        temp_val = round(random.uniform(temp[0], temp[1]), 2)
        oil_val = round(random.uniform(oil[0], oil[1]), 2)
        rpm_val = round(random.uniform(rpm[0], rpm[1]))

        client.publish(topic_temp, temp_val)
        client.publish(topic_oil, oil_val)
        client.publish(topic_rpm, rpm_val)

        print(f"[PUBLISHED] Temp: {temp_val}, Oil: {oil_val}, RPM: {rpm_val}")
        time.sleep(3)

except KeyboardInterrupt:
    print("\n[EXIT] Shutting down simulator...")
    os._exit(1)
    client.loop_stop()
    client.disconnect()
