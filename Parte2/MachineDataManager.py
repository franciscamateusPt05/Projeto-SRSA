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
        
        # Track alarm history for each machine
        self.alarm_history: Dict[str, List[dict]] = {}
        
    def _load_healthy_ranges(self, config_file: str) -> HealthyRanges:
        """Load healthy ranges from configuration file"""
        with open(config_file, 'r') as f:
            lines = f.readlines()
            
        # Parse each line (assuming fixed order as per requirements)
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
        """Configure MQTT client and connect to broker"""
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_message = self._on_message
        
        # Connect to the MQTT broker (using provided IP)
        self.mqtt_client.connect("10.6.1.9", 1883, 60)
        self.mqtt_client.loop_start()
        
    def _on_connect(self, client, userdata, flags, rc):
        """Callback when connected to MQTT broker"""
        print(f"MachineDataManager connected to MQTT broker with result code {rc}")
        
        # Subscribe to the topic where Data Manager Agent sends processed data
        topic = f"v3/{self.group_id}/machine_data"
        client.subscribe(topic)
        print(f"Subscribed to topic: {topic}")
    
    def _on_message(self, client, userdata, msg):
        """Callback when message is received from MQTT broker"""
        try:
            data = json.loads(msg.payload.decode())
            machine_id = data.get("machine_id")
            sensor_data = data.get("sensor_data")
            
            print(f"Received data from machine {machine_id}: {sensor_data}")
            
            # Analyze sensor data and send control commands if needed
            self.analyze_sensor_data(machine_id, sensor_data)
            
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON message: {e}")
        except Exception as e:
            print(f"Error processing message: {e}")
    
    def analyze_sensor_data(self, machine_id: str, sensor_data: dict):
        """Analyze sensor data and send control commands if parameters are out of range"""
        alarms = []
        
        # Check RPM
        rpm = sensor_data.get("rpm")
        if rpm is not None:
            if rpm < self.healthy_ranges.rpm_low:
                alarms.append({"parameter": "rpm", "value": rpm, "status": "low"})
            elif rpm > self.healthy_ranges.rpm_high:
                alarms.append({"parameter": "rpm", "value": rpm, "status": "high"})
        
        # Check coolant temperature
        coolant_temp = sensor_data.get("coolant_temperature")
        if coolant_temp is not None:
            if coolant_temp < self.healthy_ranges.coolant_temp_low:
                alarms.append({"parameter": "coolant_temperature", "value": coolant_temp, "status": "low"})
            elif coolant_temp > self.healthy_ranges.coolant_temp_high:
                alarms.append({"parameter": "coolant_temperature", "value": coolant_temp, "status": "high"})
        
        # Check oil pressure
        oil_pressure = sensor_data.get("oil_pressure")
        if oil_pressure is not None:
            if oil_pressure < self.healthy_ranges.oil_pressure_low:
                alarms.append({"parameter": "oil_pressure", "value": oil_pressure, "status": "low"})
            elif oil_pressure > self.healthy_ranges.oil_pressure_high:
                alarms.append({"parameter": "oil_pressure", "value": oil_pressure, "status": "high"})
        
        # Check battery potential
        battery_potential = sensor_data.get("battery_potential")
        if battery_potential is not None:
            if battery_potential < self.healthy_ranges.battery_potential_low:
                alarms.append({"parameter": "battery_potential", "value": battery_potential, "status": "low"})
            elif battery_potential > self.healthy_ranges.battery_potential_high:
                alarms.append({"parameter": "battery_potential", "value": battery_potential, "status": "high"})
        
        # Check consumption
        consumption = sensor_data.get("consumption")
        if consumption is not None:
            if consumption < self.healthy_ranges.consumption_low:
                alarms.append({"parameter": "consumption", "value": consumption, "status": "low"})
            elif consumption > self.healthy_ranges.consumption_high:
                alarms.append({"parameter": "consumption", "value": consumption, "status": "high"})
        
        # Update alarm history
        if machine_id not in self.alarm_history:
            self.alarm_history[machine_id] = []
        
        current_time = time.time()
        self.alarm_history[machine_id].extend([{"time": current_time, "alarm": alarm} for alarm in alarms])
        
        # Clean up old alarms (older than 2 minutes)
        self.alarm_history[machine_id] = [
            entry for entry in self.alarm_history[machine_id]
            if current_time - entry["time"] <= 120
        ]
        
        # Send control commands for each alarm
        for alarm in alarms:
            self.send_control_command(machine_id, alarm)
    
    def send_control_command(self, machine_id: str, alarm: dict):
        """Send control command to adjust machine parameters"""
        parameter = alarm["parameter"]
        status = alarm["status"]
        
        # Determine adjustment based on parameter and status
        if parameter == "rpm":
            if status == "high":
                # Decrease RPM by 6 (as per example)
                command = {
                    "message_type": 0x01,  # Control
                    "action_type": 0x01,   # Modify parameter
                    "parameter": 0x01,     # RPM
                    "adjustment": 0xFA     # -6
                }
            else:  # low
                # Increase RPM by 5
                command = {
                    "message_type": 0x01,
                    "action_type": 0x01,
                    "parameter": 0x01,
                    "adjustment": 0x05     # +5
                }
        
        elif parameter == "coolant_temperature":
            if status == "high":
                # Reduce load to lower temperature
                command = {
                    "message_type": 0x01,
                    "action_type": 0x01,
                    "parameter": 0x03,     # Temperature
                    "adjustment": 0xFA     # -6
                }
        
        # Similar logic for other parameters...
        
        # Send command to Data Manager Agent
        topic = f"v3/{self.group_id}/control_commands"
        self.mqtt_client.publish(topic, json.dumps({
            "machine_id": machine_id,
            "command": command
        }))
        
        print(f"Sent control command to {machine_id} for {parameter} ({status})")
    
    def run(self):
        """Main loop"""
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Shutting down MachineDataManager...")
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()

if __name__ == "__main__":
    # Group ID should be provided as argument
    import sys
    if len(sys.argv) < 2:
        print("Usage: python machine_data_manager.py <GroupID>")
        sys.exit(1)
    
    group_id = sys.argv[1]
    manager = MachineDataManager(group_id)
    manager.run()