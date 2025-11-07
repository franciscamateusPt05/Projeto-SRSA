import paho.mqtt.client as mqtt
import socket
import json
from datetime import datetime
import threading

class AlertManager:
    def __init__(self, group_id, udp_host, udp_port):
        self.group_id = group_id
        self.udp_host = udp_host
        self.udp_port = udp_port
        self.critical_params = ['coolant_temperature', 'oil_pressure']
        self.client_number = 0
        self.critical_counter = 0
        self.total_counter = 0

        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_message = self.on_mqtt_message
        self.mqtt_client.connect("10.6.1.9", 1883)
        self.mqtt_client.subscribe(f"{self.group_id}/internal/control")

        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def on_mqtt_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            machine_type = data.get('machine_type')
            parameter = data.get('parameter')

            print(f"[MQTT] {msg.topic} -> {msg.payload.decode()}")

            self.total_counter += 1
            if parameter in self.critical_params:
                self.critical_counter += 1

            status, reason = self.get_health_status()
            if status == "CRITICAL":
                self.send_alert(machine_type, status, reason)

        except Exception as e:
            print(f"[MQTT] Erro: {e}")

    def get_health_status(self):
        status = "NORMAL"
        reason = "No issues"

        if self.critical_counter > 3 and self.total_counter > 5:
            status = "CRITICAL"
            reason = "Critical parameters and total alarms exceeded"
        elif self.critical_counter > 3:
            status = "CRITICAL"
            reason = "Critical parameters exceeded"
        elif self.total_counter > 5:
            status = "CRITICAL"
            reason = "Too many alarms"

        return status, reason

    def send_alert(self, machine_type, status, reason):
        alert_msg = {
            "message_type": "alert",
            "machine_type": machine_type,
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "reason": reason
        }
        self.udp_socket.sendto(json.dumps(alert_msg).encode(), (self.udp_host, self.udp_port))
        print(f"[ALERT] Sent {status} alert for {machine_type} via UDP")

        self.critical_counter = 0
        self.total_counter = 0

    def udp_listener(self):
        while True:
            try:
                data, addr = self.udp_socket.recvfrom(1024)
                self.client_number += 1
                print(f'[UDP RECEIVED] Msg "{data.decode("utf-8")}" from Client {self.client_number} at {addr[0]}:{addr[1]}')
            except Exception as e:
                print(f"[UDP] Error receiving data: {e}")

    def run(self):
        udp_thread = threading.Thread(target=self.udp_listener, daemon=True)
        udp_thread.start()

        self.mqtt_client.loop_forever()

if __name__ == "__main__":
    alert_manager = AlertManager(group_id="15", udp_host="127.0.0.1", udp_port=5002)
    alert_manager.run()
