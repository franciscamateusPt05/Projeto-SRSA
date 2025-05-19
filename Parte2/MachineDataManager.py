import paho.mqtt.client as mqtt
import json
import time
from dataclasses import dataclass
from typing import Dict, List

@dataclass
class HealthyRanges:
    rpm_low: float
    rpm_high: float
    rpm_ideal: float
    coolant_temp_low: float
    coolant_temp_high: float
    coolant_temp_ideal: float
    oil_pressure_low: float
    oil_pressure_high: float
    oil_pressure_ideal: float
    battery_potential_low: float
    battery_potential_high: float
    battery_potential_ideal: float
    consumption_low: float
    consumption_high: float
    consumption_ideal: float

class MachineDataManager:
    def __init__(self, group_id: str, config_file: str = "intervals.cfg"):
        self.group_id = group_id
        self.healthy_ranges = self._load_healthy_ranges(config_file)
        self.mqtt_client = mqtt.Client()
        self.setup_mqtt()
        
    def _load_healthy_ranges(self, config_file: str) -> HealthyRanges:
        with open(config_file, 'r') as f:
            lines = f.readlines()

        rpm = list(map(float, lines[0].split('#')[0].split()))
        coolant_temp = list(map(float, lines[1].split('#')[0].split()))
        oil_pressure = list(map(float, lines[2].split('#')[0].split()))
        battery_potential = list(map(float, lines[3].split('#')[0].split()))
        consumption = list(map(float, lines[4].split('#')[0].split()))
        
        return HealthyRanges(
            rpm_low=rpm[0], rpm_high=rpm[1], rpm_ideal=rpm[2],
            coolant_temp_low=coolant_temp[0], coolant_temp_high=coolant_temp[1], 
            coolant_temp_ideal=coolant_temp[2],
            oil_pressure_low=oil_pressure[0], oil_pressure_high=oil_pressure[1],
            oil_pressure_ideal=oil_pressure[2],
            battery_potential_low=battery_potential[0], 
            battery_potential_high=battery_potential[1],
            battery_potential_ideal=battery_potential[2],
            consumption_low=consumption[0], consumption_high=consumption[1],
            consumption_ideal=consumption[2]
        )
    
    def setup_mqtt(self):
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_message = self._on_message
        
        # Connect to the MQTT broker (using provided IP)
        self.mqtt_client.connect("10.6.1.9", 1883, 60)
        self.mqtt_client.loop_start()

    def _on_connect(self, client, userdata, flags, rc):
        print(f"MachineDataManager connected to MQTT broker with result code {rc}")

        topic = f"{self.group_id}/internal/machine_data"
        client.subscribe(topic)
        print(f"Subscribed to topic: {topic}")

    def _on_message(self, client, userdata, msg):
        """Callback when message is received from MQTT broker"""
        try:
            data = json.loads(msg.payload.decode())
            
            print(f"Received data from machine {data['machine_type']}")

            self.analyze_sensor_data(data)
            
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON message: {e}")
        except Exception as e:
            print(f"Error processing message: {e}")
    
    def analyze_sensor_data(self, data):
        alarms = []
        machine_type=data["machine_type"]

        rpm = data.get("rpm")
        if rpm is not None:
            if rpm < self.healthy_ranges.rpm_low:
                alarms.append({"parameter": "rpm", "value": rpm, "status": "low","correction":self.healthy_ranges.rpm_ideal-rpm})
            elif rpm > self.healthy_ranges.rpm_high:
                alarms.append({"parameter": "rpm", "value": rpm, "status": "high","correction":self.healthy_ranges.rpm_ideal-rpm})

        coolant_temp = data.get("coolant_temperature")
        if coolant_temp is not None:
            if coolant_temp < self.healthy_ranges.coolant_temp_low:
                alarms.append({"parameter": "coolant_temperature", "value": coolant_temp, "status": "low","correction":self.healthy_ranges.coolant_temp_ideal-coolant_temp})
            elif coolant_temp > self.healthy_ranges.coolant_temp_high:
                alarms.append({"parameter": "coolant_temperature", "value": coolant_temp, "status": "high","correction":self.healthy_ranges.coolant_temp_ideal-coolant_temp})

        oil_pressure = data.get("oil_pressure")
        if oil_pressure is not None:
            if oil_pressure < self.healthy_ranges.oil_pressure_low:
                alarms.append({"parameter": "oil_pressure", "value": oil_pressure, "status": "low","correction":self.healthy_ranges.oil_pressure_ideal-oil_pressure})
            elif oil_pressure > self.healthy_ranges.oil_pressure_high:
                alarms.append({"parameter": "oil_pressure", "value": oil_pressure, "status": "high","correction":self.healthy_ranges.oil_pressure_ideal-oil_pressure})

        battery_potential = data.get("battery_potential")
        if battery_potential is not None:
            if battery_potential < self.healthy_ranges.battery_potential_low:
                alarms.append({"parameter": "battery_potential", "value": battery_potential, "status": "low","correction":self.healthy_ranges.battery_potential_ideal-battery_potential})
            elif battery_potential > self.healthy_ranges.battery_potential_high:
                alarms.append({"parameter": "battery_potential", "value": battery_potential, "status": "high","correction":self.healthy_ranges.battery_potential_ideal-battery_potential})

        consumption = data.get("consumption")
        if consumption is not None:
            if consumption < self.healthy_ranges.consumption_low:
                alarms.append({"parameter": "consumption", "value": consumption, "status": "low","correction":self.healthy_ranges.consumption_ideal-consumption})
            elif consumption > self.healthy_ranges.consumption_high:
                alarms.append({"parameter": "consumption", "value": consumption, "status": "high","correction":self.healthy_ranges.consumption_ideal-consumption})

        for alarm in alarms:
            alarm['machine_type'] = machine_type
            topic = f"{self.group_id}/internal/control"
            self.mqtt_client.publish(topic, json.dumps(alarm))
    
    def run(self):
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Shutting down MachineDataManager...")
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()

if __name__ == "__main__":
    group_id = 15
    manager = MachineDataManager(group_id)
    manager.run()