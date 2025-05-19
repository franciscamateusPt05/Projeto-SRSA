import paho.mqtt.client as mqtt
import datetime
import json


class MQTTDebugger:
    def __init__(self, broker_address, broker_port, group_id):
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.group_id = group_id
        self.client = mqtt.Client()

        # Set up callback functions
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

    def on_connect(self, client, userdata, flags, rc):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}]: Connected to MQTT broker with result code {rc}")

        # Subscribe to all relevant topics using wildcards
        client.subscribe(f"v3/{self.group_id}@ttn/devices/+/up")
        client.subscribe(f"v3/{self.group_id}@ttn/devices/+/down/+")
        client.subscribe(f"{self.group_id}/+/+")

    def on_message(self, client, userdata, msg):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            message_content = json.loads(msg.payload.decode())
        except json.JSONDecodeError as e:
            print(e)

        print(f"[{current_time}]:[{msg.topic}]")

    def on_disconnect(self, client, userdata, rc):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}]: Disconnected from MQTT broker with result code {rc}")

    def start(self):
        print("Starting MQTT Debugger...")
        self.client.connect(self.broker_address, self.broker_port)
        self.client.loop_forever()

    def stop(self):
        self.client.disconnect()


if __name__ == "__main__":
    BROKER_ADDRESS = "10.6.1.9"
    BROKER_PORT = 1883
    GROUP_ID = "15"

    debugger = MQTTDebugger(BROKER_ADDRESS, BROKER_PORT, GROUP_ID)

    try:
        debugger.start()
    except KeyboardInterrupt:
        debugger.stop()
        print("MQTT Debugger stopped by user")