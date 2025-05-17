import paho.mqtt.client as mqtt
import socket
import json
import time
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class DataManagerAgent:
    def __init__(self, group_id, machine_ids, broker_ip="10.6.1.9", broker_port=1883):
        self.group_id = group_id
        self.machine_ids = machine_ids
        self.broker_ip = broker_ip
        self.broker_port = broker_port
        
        # MQTT Client setup
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message
        
        # UDP setup for Alert Manager
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_port = 10000  # Example port, can be configured
        self.udp_socket.bind(('localhost', self.udp_port))
        
        # Internal state
        self.CodeMachine={"A23X":"M1","B47Y":"M2","C89Z":"M3","D56W":"M4","E34V":"M5","F78T":"M6","G92Q":"M7","H65P":"M8"}
        self.machine_data = {mid: {} for mid in machine_ids}
        self.control_topic = f"v3/{self.group_id}/internal/control"
        
    def on_mqtt_connect(self, client, userdata, flags, rc):
        print(f"Connected to MQTT broker with result code {rc}")
        
        # Subscribe to machine data topics
        
        topic = f"v3/{self.group_id}@ttn/devices/#/up"
        client.subscribe(topic)
        
        # Subscribe to internal control topic
        client.subscribe(self.control_topic)
        
    def on_mqtt_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            topic = msg.topic
            
            print(f"[DEBUG] Received MQTT message on {topic}: {payload}")
            
            if "up" in topic:  # Message from machine
                self.process_machine_message(payload, topic)
            elif topic == self.control_topic:
                self.process_control_message(payload)
                
        except Exception as e:
            print(f"Error processing MQTT message: {e}")
    
    def process_machine_message(self, payload, topic):
        machine_id = topic.split('/')[4]
        
        # Extract and standardize sensor data
        decoded = payload.get('uplink_message', {}).get('decoded_payload', {})
        self.machine_data[machine_id] = {
            'timestamp': datetime.now().isoformat(), #Este é quando recebo devia ser o quando foi retirado o valor?
            'rpm': decoded.get('rpm'),
            'coolant_temp': decoded.get('coolant_temperature'),
            'oil_pressure': decoded.get('oil_pressure'),
            'battery_pot': decoded.get('battery_potential'),
            'consumption': decoded.get('consumption'),
            'machine_type': decoded.get('machine_type'),
            'rssi': payload.get('rx_metadata', [{}])[0].get('rssi'),
            'snr': payload.get('rx_metadata', [{}])[0].get('snr'),
            'channel_rssi': payload.get('rx_metadata', [{}])[0].get('channel_rssi')
        }
        self.standardize_units(machine_id)
        
        # Forward to Machine Data Manager
        self.send_to_data_manager(self.machine_data[machine_id])
        
        # Store in InfluxDB
        self.store_in_database(self.machine_data[machine_id])
    
    def standardize_units(self, machine_id):
        data = self.machine_data[machine_id]
        machine_type = data['machine_type']
        if machine_type in ['A23X','C89Z','E34V','H65P']:
            if 'oil_pressure' in data:
                data['oil_pressure'] = data['oil_pressure']*0.0689476
        if machine_type in ['E34V', 'G92Q', 'F78T', 'H65P']:
            if 'coolant_temp' in data:
                data['coolant_temp'] = (data['coolant_temp'] - 32) * 5 / 9
        if machine_type in ['H65P']:
            if 'battery_potential' in data:
                data['battery_potential'] = data['battery_potential']/1000
        if machine_type in ['C89Z','B47Y','E34V','H65P']:
            if 'battery_potential' in data:
                data['battery_potential'] = data['battery_potential']/1000

    def send_to_data_manager(self, data):
        topic = f"{self.group_id}/internal/machine_data"
        self.mqtt_client.publish(topic, json.dumps(data))
    
    def process_control_message(self, payload):
        machine_type = payload.get('machine_type')
        action = payload.get('action')
        parameter = payload.get('parameter')
        corretion=payload.get('corretion')
        
        # Encode control message
        encoded = self.encode_control_message(action, parameter,corretion)
        
        machine_id=self.CodeMachine[machine_type]
        topic = f"v3/{self.group_id}@ttn/devices/{machine_id}/down/push_machine"
        downlink_msg = {
            "downlinks": [{
                "frm_payload": encoded,
                "f_port": 10,
                "priority": "NORMAL"
            }]
        }
        self.mqtt_client.publish(topic, json.dumps(downlink_msg))
    
    def encode_control_message(self, action, parameter, value):
        # Implement byte encoding based on project specs
        # Example: RPM reduction by 6
        # 0x01 0x01 0x01 0xFA
        message_type = 0x01  # Control
        action_type = 0x01   # Modify parameter
        
        # Map parameter to byte code
        param_map = {
            'rpm': 0x01,
            'fuel': 0x02,
            'temperature': 0x03,
            # Add other parameters
        }
        
        param_byte = param_map.get(parameter, 0x00)
        value_byte = self.convert_to_signed_byte(value)
        
        return f"0x{message_type:02x} 0x{action_type:02x} 0x{param_byte:02x} 0x{value_byte:02x}"
    
    def convert_to_signed_byte(self, value):
        # Convert value to signed byte (-128 to 127)
        return value & 0xff
    
    def process_udp_alerts(self):
        while True:
            data, addr = self.udp_socket.recvfrom(1024)
            try:
                alert = json.loads(data.decode())
                self.handle_alert(alert)
            except Exception as e:
                print(f"Error processing UDP alert: {e}")
    
    def handle_alert(self, alert):
        machine_id = alert.get('machine_id')
        alert_level = alert.get('level')  # NORMAL, CRITICAL
        reason = alert.get('reason')
        
        if alert_level == "CRITICAL":
            # Encode alert message
            encoded = self.encode_alert_message(reason)
            
            # Send to machine
            topic = f"v3/{self.group_id}@ttn/devices/{machine_id}/down/push_alert"
            downlink_msg = {
                "downlinks": [{
                    "frm_payload": encoded,
                    "f_port": 10,
                    "priority": "NORMAL"
                }]
            }
            self.mqtt_client.publish(topic, json.dumps(downlink_msg))
    
    def encode_alert_message(self, reason):
        # Implement byte encoding for alerts
        # Example: Stop machine due to high temperature
        # 0x02 0x01 0x01
        message_type = 0x02  # Alert
        action_type = 0x01   # Stop machine
        
        # Map reason to byte code
        reason_map = {
            'high_temp': 0x01,
            'low_pressure': 0x02,
            # Add other reasons
        }
        
        reason_byte = reason_map.get(reason, 0x00)
        
        return f"0x{message_type:02x} 0x{action_type:02x} 0x{reason_byte:02x}"
    
    def store_in_database(self, data):


        # Configure with your InfluxDB Cloud credentials
        token = "your-token"
        org = "your-org"
        bucket = "your-bucket"
        url = "https://us-west-2-1.aws.cloud2.influxdata.com"
        
        with InfluxDBClient(url=url, token=token, org=org) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            
            point = Point("machine_data") \
                .tag("machine_id", data.get('machine_id')) \
                .tag("machine_type", data.get('machine_type')) \
                .field("rpm", data.get('rpm')) \
                .field("coolant_temp", data.get('coolant_temp')) \
                .field("oil_pressure", data.get('oil_pressure')) \
                .field("battery_pot", data.get('battery_pot')) \
                .field("consumption", data.get('consumption')) \
                .field("rssi", data.get('rssi')) \
                .field("snr", data.get('snr')) \
                .field("channel_rssi", data.get('channel_rssi')) \
                .time(datetime.utcnow(), WritePrecision.NS)
            
            write_api.write(bucket, org, point)
    
    def start(self):
        # Connect to MQTT broker
        self.mqtt_client.connect(self.broker_ip, self.broker_port, 60)
        
        # Start MQTT loop in a separate thread
        self.mqtt_client.loop_start()
        
        # Start UDP listener
        import threading
        udp_thread = threading.Thread(target=self.process_udp_alerts, daemon=True)
        udp_thread.start()
        
        print("Data Manager Agent started")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Shutting down...")
            self.mqtt_client.loop_stop()
            self.udp_socket.close()

if __name__ == "__main__":
    # Example usage
    group_id = "your-group-id"  # Replace with your group ID
    machine_ids = ["M1", "M2", "M3", "M4", "M5", "M6", "M7", "M8"]  # All machine IDs
    
    agent = DataManagerAgent(group_id, machine_ids)
    agent.start()