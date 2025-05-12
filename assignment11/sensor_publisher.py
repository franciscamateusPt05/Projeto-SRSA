import random
import paho.mqtt.client as mqtt
import sys
import time
import json


GroupID = sys.argv[1]
topic = f'Group{GroupID}/sensor_data'

broker = "10.6.1.9"
port = 1883

client = mqtt.Client()
client.connect(broker,port)
client.loop_start()

try:
    while True:
        temperature = random.randint(15,30)
        humidity = random.randint(30,70)
        motion = random.choice(["Detected","None"])
        data = {"Temperature":temperature,"Humidity":humidity,"Motion":motion}

        client.publish(topic,json.dumps(data))
        print(f"Published: {data}")
        time.sleep(5)

except KeyboardInterrupt:
    print("Exiting")
finally:
    client.loop_stop()
    client.disconnect()

