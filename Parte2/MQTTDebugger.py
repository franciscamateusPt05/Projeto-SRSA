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
        """Callback when connected to the MQTT broker"""
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}]: Connected to MQTT broker with result code {rc}")

        # Subscribe to all relevant topics using wildcards
        client.subscribe(f"v3/{self.group_id}@ttn/devices/+/up")  # Machine data topics
        client.subscribe(f"v3/{self.group_id}@ttn/devices/+/down/+")  # Control/alert topics
        client.subscribe(f"{self.group_id}/+/+")  # Internal communication topics

    def on_message(self, client, userdata, msg):
        """Callback when a message is received"""
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            # Try to parse message as JSON
            message_content = json.loads(msg.payload.decode())
            formatted_message = json.dumps(message_content, indent=2)
        except json.JSONDecodeError:
            # If not JSON, display raw message
            formatted_message = msg.payload.decode()

        print(f"[{current_time}]:[{msg.topic}]:\n{formatted_message}\n")

    def on_disconnect(self, client, userdata, rc):
        """Callback when disconnected from the MQTT broker"""
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}]: Disconnected from MQTT broker with result code {rc}")

    def start(self):
        """Start the MQTT debugger"""
        print("Starting MQTT Debugger...")
        self.client.connect(self.broker_address, self.broker_port)
        self.client.loop_forever()

    def stop(self):
        """Stop the MQTT debugger"""
        self.client.disconnect()


if __name__ == "__main__":
    # Configuration - these should be passed as parameters or from config file
    BROKER_ADDRESS = "10.6.1.9"  # srsa-pi-8.dei.ac.pt
    BROKER_PORT = 1883
    GROUP_ID = "100"  # Example group ID - should be provided

    debugger = MQTTDebugger(BROKER_ADDRESS, BROKER_PORT, GROUP_ID)

    try:
        debugger.start()
    except KeyboardInterrupt:
        debugger.stop()
        print("MQTT Debugger stopped by user")