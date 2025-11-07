import threading

import paho.mqtt.client as mqtt
import socket
import json
import time
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class DataManagerAgent:
    def __init__(self, group_id, broker_ip="10.6.1.9", broker_port=1883):
        self.group_id = group_id
        self.broker_ip = broker_ip
        self.broker_port = broker_port
        
        # MQTT Client setup
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message

        self.udp_host='127.0.0.1'
        self.udp_port = 5002
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind((self.udp_host, self.udp_port))

        self.CodeMachine={"A23X":"M1","B47Y":"M2","C89Z":"M3","D56W":"M4","E34V":"M5","F78T":"M6","G92Q":"M7","H65P":"M8"}
        self.machine_ids=self.CodeMachine.values()
        self.machine_data = {mid: {} for mid in self.machine_ids}
        self.control_topic =f"{self.group_id}/internal/control"
        
    def on_mqtt_connect(self, client, userdata, flags, rc):
        print(f"Connected to MQTT broker with result code {rc}")

        topic = f"v3/{self.group_id}@ttn/devices/+/up"
        client.subscribe(topic)

        client.subscribe(self.control_topic)
        
    def on_mqtt_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            topic = msg.topic
            
            if "up" in topic:
                self.process_machine_message(payload, topic)
            elif topic == self.control_topic:
                self.process_control_message(payload)
                
        except Exception as e:
            print(f"Error processing MQTT message: {e}")
    
    def process_machine_message(self, payload, topic):
        machine_id = topic.split('/')[4]
        decoded = payload.get('uplink_message', {}).get('decoded_payload', {})
        self.machine_data[machine_id] = {
            'timestamp': datetime.now().isoformat(),
            'rpm': decoded.get('rpm'),
            'coolant_temperature': decoded.get('coolant_temperature'),
            'oil_pressure': decoded.get('oil_pressure'),
            'battery_potencial': decoded.get('battery_potential'),
            'consumption': decoded.get('consumption'),
            'machine_type': decoded.get('machine_type'),
            'rssi': payload.get('uplink_message', {}).get('rx_metadata', [{}])[0].get('rssi'),
            'snr': payload.get('uplink_message', {}).get('rx_metadata', [{}])[0].get('snr'),
            'channel_rssi': payload.get('uplink_message', {}).get('rx_metadata', [{}])[0].get('channel_rssi'),

        }
        self.standardize_units(machine_id)
        self.send_to_data_manager(self.machine_data[machine_id])

        self.store_in_database(self.machine_data[machine_id])
    
    def standardize_units(self, machine_id):
        data = self.machine_data[machine_id]
        machine_type = data['machine_type']
        if machine_type in ['A23X','C89Z','E34V','H65P']:
            if 'oil_pressure' in data:
                data['oil_pressure'] = data['oil_pressure']/ 14.5038
        if machine_type in ['E34V', 'G92Q', 'F78T', 'H65P']:
            if 'coolant_temperature' in data:
                data['coolant_temperature'] = (data['coolant_temperature'] - 32) * 5 / 9
        if machine_type in ['H65P']:
            if 'battery_potential' in data:
                data['battery_potential'] = data['battery_potential']/1000
        if machine_type in ['C89Z','B47Y','E34V','H65P']:
            if 'consumption' in data:
                data['consumption'] = data['consumption'] * 3.78541

    def send_to_data_manager(self, data):
        topic = f"{self.group_id}/internal/machine_data"
        self.mqtt_client.publish(topic, json.dumps(data))
    
    def process_control_message(self, payload):
        machine_type = payload.get('machine_type')
        parameter = payload.get('parameter')
        corretion=payload.get('correction')
        
        # Encode control message
        encoded = self.encode_control_message(machine_type, parameter, corretion)
        machine_id=self.CodeMachine[machine_type]
        topic = f"v3/{self.group_id}@ttn/devices/{machine_id}/down/push_actuator"
        downlink_msg = {
            "downlinks": [{
                "frm_payload": encoded,
                "f_port": 10,
                "priority": "NORMAL"
            }]
        }
        self.mqtt_client.publish(topic, json.dumps(downlink_msg))
        self.store_control_message_in_database(machine_type, parameter, corretion)

    def encode_control_message(self,machine_type, parameter,corretion):
        message_type = 0x01
        action_type = 0x01

        param_map = {
            'rpm': 0x01,
            'oil_pressure': 0x02,
            'coolant_temperature': 0x03,
            'battery_potential': 0x04,
            'consumption': 0x05
}
        
        param_byte = param_map.get(parameter, 0x00)
        value_byte = self.convert_to_signed_byte(self.revert_to_original_units(machine_type, parameter, int(corretion)))
        
        return f"0x{message_type:02x} 0x{action_type:02x} 0x{param_byte:02x} 0x{value_byte:02x}"
    
    def convert_to_signed_byte(self, value):
        return int(value) & 0xff

    def process_udp_alerts(self):
        print(f"[UDP] A ouvir em {self.udp_host}:{self.udp_port}...")
        while True:
            try:
                data, addr = self.udp_socket.recvfrom(4096)
                message = data.decode()
                alert = json.loads(message)
                print(f"[UDP] Received Alert: {alert}")

                self.handle_alert(alert)

            except json.JSONDecodeError as json_err:
                print(f"[UDP] Erro ao decodificar JSON: {json_err}")
                print(f"[UDP] Dados brutos: {data}")

            except Exception as e:
                print(f"[UDP] Erro inesperado ao processar alerta: {e}")

    def handle_alert(self, alert):
        machine_type = alert.get('machine_type')
        machine_id = self.CodeMachine[machine_type]
        alert_level = alert.get('status')
        reason = alert.get('reason')
        
        if alert_level == "CRITICAL":
            # Encode alert message
            encoded = self.encode_alert_message(reason)
            print(encoded)

            topic = f"v3/{self.group_id}@ttn/devices/{machine_id}/down/push_alert"
            downlink_msg = {
                "downlinks": [{
                    "frm_payload": encoded,
                    "f_port": 10,
                    "priority": "NORMAL"
                }]
            }
            try:
                self.mqtt_client.publish(topic, json.dumps(downlink_msg))
            except Exception as e:
                print(f"[UDP] Erro ao processar alerta: {e}")
            self.store_alert_message_in_database(machine_type, reason, alert_level)
    def revert_to_original_units(self, machine_type, parameter, corretion):

        if machine_type in ['A23X', 'C89Z', 'E34V', 'H65P']:
            if 'oil_pressure' == parameter:
                corretion = corretion * 14.5038

        if machine_type in ['E34V', 'G92Q', 'F78T', 'H65P']:
            if 'coolant_temperature' == parameter:
                if corretion < 0:
                    corretion = -((abs(corretion) * 9 / 5) + 32)
                else:
                    corretion = (corretion * 9 / 5) + 32

        if machine_type in ['H65P']:
            if 'battery_potential' == parameter:
                corretion = corretion * 1000

        if machine_type in ['C89Z', 'B47Y', 'E34V', 'H65P']:
            if 'consumption' == parameter:
                corretion = corretion / 3.78541
        return corretion
    
    def encode_alert_message(self, reason):
        message_type = 0x02
        action_type = 0x01

        reason_map = {
            'Critical parameters and total alarms exceeded': 0x01,
            'Critical parameters exceeded': 0x02,
            'Too many alarms': 0x03,
        }
        
        reason_byte = reason_map.get(reason, 0x00)
        
        return f"0x{message_type:02x} 0x{action_type:02x} 0x{reason_byte:02x}"
    
    def store_in_database(self, data):
        token = "7WCrrZwZ99icYE8XxxzEXM8EMxjCVwRLQjSM07nurpl0MC91DgytxcuLHimheXi1MI414_1puHHa2z9rq1qHFg=="
        org = "SRSA"
        bucket = "projetoSRSA"
        url = "https://us-east-1-1.aws.cloud2.influxdata.com"
        
        with InfluxDBClient(url=url, token=token, org=org) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)

            point = Point("machine_data") \
                .tag("machine_id", data.get('machine_id')) \
                .tag("machine_type", data.get('machine_type')) \
                .field("rpm", float(data.get('rpm', 0.0))) \
                .field("coolant_temperature", float(data.get('coolant_temperature', 0.0))) \
                .field("oil_pressure", float(data.get('oil_pressure', 0.0))) \
                .field("battery_potencial", float(data.get('battery_potencial', 0.0))) \
                .field("consumption", float(data.get('consumption', 0.0))) \
                .field("rssi", float(data.get('rssi', 0.0))) \
                .field("snr", float(data.get('snr', 0.0))) \
                .field("channel_rssi", float(data.get('channel_rssi', 0.0))) \
                .time(datetime.utcnow(), WritePrecision.NS)

            write_api.write(bucket, org, point)

    def store_control_message_in_database(self, machine_type, parameter, corretion):
        token = "7WCrrZwZ99icYE8XxxzEXM8EMxjCVwRLQjSM07nurpl0MC91DgytxcuLHimheXi1MI414_1puHHa2z9rq1qHFg=="
        org = "SRSA"
        bucket = "projetoSRSA"
        url = "https://us-east-1-1.aws.cloud2.influxdata.com"

        with InfluxDBClient(url=url, token=token, org=org) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)

            point = Point("control_messages") \
                .tag("machine_type", machine_type) \
                .tag("parameter", parameter) \
                .field("correction_value", int(corretion)) \
                .time(datetime.utcnow(), WritePrecision.NS)

            write_api.write(bucket=bucket, org=org, record=point)

    def store_alert_message_in_database(self, machine_type, reason, status):
        token = "7WCrrZwZ99icYE8XxxzEXM8EMxjCVwRLQjSM07nurpl0MC91DgytxcuLHimheXi1MI414_1puHHa2z9rq1qHFg=="
        org = "SRSA"
        bucket = "projetoSRSA"
        url = "https://us-east-1-1.aws.cloud2.influxdata.com"

        with InfluxDBClient(url=url, token=token, org=org) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)

            point = Point("alert_messages") \
                .tag("machine_type", machine_type) \
                .tag("status", status) \
                .field("reason", reason) \
                .time(datetime.utcnow(), WritePrecision.NS)

            write_api.write(bucket=bucket, org=org, record=point)

    def start(self):
        self.mqtt_client.connect(self.broker_ip, self.broker_port, 60)
        self.mqtt_client.loop_start()

        udp_thread = threading.Thread(target=self.process_udp_alerts(), daemon=True)
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
    group_id = "15"
    agent = DataManagerAgent(group_id)
    agent.start()