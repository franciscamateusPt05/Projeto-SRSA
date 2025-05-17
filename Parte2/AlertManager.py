import paho.mqtt.client as mqtt
import socket
import json
from collections import deque
from datetime import datetime, timedelta
import time

class AlertManager:
    def __init__(self, group_id, udp_host='localhost', udp_port=5005):
        self.group_id = group_id
        self.udp_host = udp_host
        self.udp_port = udp_port
        self.alarm_history = deque(maxlen=100)  # Stores (timestamp, machine_id, parameter)
        self.critical_params = ['coolant_temperature', 'oil_pressure']  # Most critical parameters
        
        # MQTT client setup
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_message = self.on_mqtt_message
        self.mqtt_client.connect("10.6.1.9", 1883)
        self.mqtt_client.subscribe(f"v3/{self.group_id}/machine_control")
        
        # UDP socket setup
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
    def on_mqtt_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            machine_id = data['machine_id']
            parameter = data['parameter']
            action = data['action']
            
            # Record alarm if it's a control action
            if action == "adjust":
                self.alarm_history.append((datetime.now(), machine_id, parameter))
                print(f"Recorded alarm for {machine_id} - {parameter}")
                
        except Exception as e:
            print(f"Error processing MQTT message: {e}")
    
    def assess_machine_health(self, machine_id):
        # Get alarms for this machine in last 2 minutes
        time_window = datetime.now() - timedelta(minutes=2)
        recent_alarms = [
            alarm for alarm in self.alarm_history 
            if alarm[0] > time_window and alarm[1] == machine_id
        ]
        
        # Count critical alarms
        critical_count = sum(1 for alarm in recent_alarms if alarm[2] in self.critical_params)
        total_count = len(recent_alarms)
        
        # Determine health status
        if critical_count > 3 or total_count > 5:
            return "CRITICAL"
        elif total_count > 2:
            return "WARNING"
        else:
            return "NORMAL"
    
    def send_alert(self, machine_id, status):
        alert_msg = {
            "message_type": "alert",
            "machine_id": machine_id,
            "status": status,
            "timestamp": datetime.now().isoformat()
        }
        self.udp_socket.sendto(
            json.dumps(alert_msg).encode(), 
            (self.udp_host, self.udp_port)
        )
        print(f"Sent {status} alert for {machine_id} via UDP")
    
    def run(self):
        self.mqtt_client.loop_start()
        try:
            while True:
                # Check all machines periodically
                # In a real implementation, we'd track which machines are active
                for machine_id in [f"M{i}" for i in range(1, 9)]:
                    status = self.assess_machine_health(machine_id)
                    if status == "CRITICAL":
                        self.send_alert(machine_id, status)
                time.sleep(30)  # Check every 30 seconds
        except KeyboardInterrupt:
            self.mqtt_client.loop_stop()

if __name__ == "__main__":
    # Example usage - GroupID should be provided as argument
    alert_manager = AlertManager(group_id="100")
    alert_manager.run()