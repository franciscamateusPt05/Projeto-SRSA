import paho.mqtt.client as mqtt
import socket
import json
from datetime import datetime, timedelta
import time


class AlertManager:
    def __init__(self, group_id, udp_host='localhost', udp_port=5005):
        self.alarm_history = {}  # Inicializar como dicionário aninhado
        self.group_id = group_id
        self.udp_host = udp_host
        self.udp_port = udp_port
        self.critical_params = ['coolant_temperature', 'oil_pressure']

        # MQTT client setup
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_message = self.on_mqtt_message
        self.mqtt_client.connect("10.6.1.9", 1883)
        self.mqtt_client.subscribe(f"{self.group_id}/machine_control")

        # UDP socket setup
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def on_mqtt_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            machine_type = data['machine_type']
            parameter = data['parameter']

            # Inicializar máquina e parâmetro, se necessário
            if machine_type not in self.alarm_history:
                self.alarm_history[machine_type] = {}
            if parameter not in self.alarm_history[machine_type]:
                self.alarm_history[machine_type][parameter] = []

            # Registar timestamp do alarme
            self.alarm_history[machine_type][parameter].append(datetime.now())

            # Verificar estado de saúde
            status = self.get_health_status(machine_type)
            if status in ["WARNING", "CRITICAL"]:
                self.send_alert(machine_type, status)

        except Exception as e:
            print(f"Error processing MQTT message: {e}")

    def get_health_status(self, machine_type):
        time_window = datetime.now() - timedelta(minutes=2)
        critical_count = 0
        total_count = 0

        if machine_type in self.alarm_history:
            for parameter, timestamps in self.alarm_history[machine_type].items():
                recent_times = [t for t in timestamps if t > time_window]
                total_count += len(recent_times)
                if parameter in self.critical_params:
                    critical_count += len(recent_times)

        if critical_count > 3 or total_count > 5:
            return "CRITICAL"
        elif total_count > 2:
            return "WARNING"
        else:
            return "NORMAL"

    def send_alert(self, machine_type, status):
        alert_msg = {
            "message_type": "alert",
            "machine_type": machine_type,
            "status": status,
            "timestamp": datetime.now().isoformat()
        }
        self.udp_socket.sendto(json.dumps(alert_msg).encode(), (self.udp_host, self.udp_port))
        print(f"Sent {status} alert for {machine_type} via UDP")

    def run(self):
        self.mqtt_client.loop_forever()


if __name__ == "__main__":
    alert_manager = AlertManager(group_id="100")
    alert_manager.run()
