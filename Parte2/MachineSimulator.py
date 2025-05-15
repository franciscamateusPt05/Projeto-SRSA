import sys
import time
import json
import random 
from datetime import datetime
import paho.mqtt.client as mqtt

# Config do ficheiro
GroupID = sys.argv[1]
MACHINE_UPDATE_TIME = sys.argv[2] # pode ser alterado 
Machine_Code = sys.argv[3]

Estragada = sys.argv[4]

# TOPICOS
# para enviar
network_topic = f'v3/{GroupID}@ttn/devices/{MachineID}/up'
# para receber
DataManagerAgent_topic = f'v3/{GroupID}@ttn/devices/{MahcineID}/down/push_actuator'
AlertManger_topic = f'v3/{GroupID}@ttn/devices/{MachineID}/down/push_alert'


mqtthost = "10.6.1.9"
mqttport = 1883


client = mqtt.Client()



unidades = {"OilPressure":[0,1,0,1,0,1,0,1], # 1 significa que está de acordo com o SI 
            "CoolantTemp":[1,1,1,1,0,0,0,0],
            "BatteryPotential":[1,1,1,1,1,1,1,1],
            "Consumption":[1,0,0,1,0,1,1,0]
            }

CodeMachine={"A23X":1,"B47Y":2,"C89Z":3,"D56W":4,"E34V":5,"F78T":6,"G92Q":7,"H65P":8}


starttemp = 92 if unidades[CoolantTemp][1] else 197.6
startpressure = 3.2 if unidades[CoolantTemp][1] else 197.6
startpotential = 12.6 if unidades[CoolantTemp][1] else 197.6
startConsumption = 15.82 if unidades[CoolantTemp][1] else 197.6



Machine_Data= { 

  "end_device_ids": { 
    "machine_id": f'M{CodeMachine[Machine_Code]}', 
    "application_id": "my-application", 
    "dev_eui": "70B3D57ED00347C5", 
    "join_eui": "0000000000000000", 
    "dev_addr": "260B1234" 
  }, 
  "received_at": datetime.now(), 
  "uplink_message": { 
    "f_port": 1, 
    "f_cnt": 1234, 
    "frm_payload": "BASE64_ENCODED_PAYLOAD", 
    "decoded_payload": { 
      "rpm": 2000.0, 
      "coolant_temperature": starttemp, 
      "oil_pressure": startpressure, 
      "battery_potential": startpotential, 
      "consumption": startConsumption, 
      "machine_type": Machine_Code 
    }, 
    "rx_metadata": [ 
      { 
        "gateway_id": "gateway-1", 
        "rssi": -75, 
        "snr": 9.2, 
        "channel_rssi": -76, 
        "uplink_token": "TOKEN_VALUE" 
      } 
    ], 
    "settings": { 
      "data_rate": { 
        "modulation": "LORA", 
        "bandwidth": 125000, 
        "spreading_factor": 7 
      }, 
      "frequency": "868300000", 
      "timestamp": 1234567890 
    }, 
    "consumed_airtime": "0.061696s" 
  } 
}


def generateRPM(alarmon:bool,toadd = 0):
  global Machine_Data
  if alarmon:
    rpm  = Machine_Data["decoded_payload"]['rpm'] + toadd
  elif !brokenmachine:
    rpm = Mahcine_Data["decodec_payload"]['rpm'] + random.choice([-50,200])
  else:
    rpm = Machine_Data["decoded_payload"]['rpm'] + 100

    Machine_Date["decoded_payload"]['rpm'] = max(800,min(rpm,3000))


def generateOilPressure(alarmon:bool,toadd = 0):
  global Machine_Data
  if alarmon:
    oilpressure  = Machine_Data["decoded_payload"]['oil_pressure'] + toadd
  elif !brokenmachine:
    oilpressure = Mahcine_Data["decodec_payload"]['oil_pressure'] + random.choice([-0.1,0.5])
  else:
    oilpressure = Machine_Data["decoded_payload"]['oil_pressure'] + 1.0

    Machine_Date["decoded_payload"]['oil_pressure'] = max(1.5,min(oilpressure,8.0))



def generatePotential(alarmon:bool,toadd = 0):
  global Machine_Data
  if alarmon:
    batterypotential  = Machine_Data["decoded_payload"]['rpm'] + toadd
  elif !brokenmachine:
    batterypotential = Mahcine_Data["decodec_payload"]['rpm'] + random.choice([-50,200])
  else:
    batterypotential = Machine_Data["decoded_payload"]['rpm'] + 100
    batterypotential = max(10,min(battery_potential),14)
    Machine_Date["decoded_payload"]['battery_potential'] = batterypotential



def generateRPM(alarmon:bool,toadd = 0):
  global Machine_Data
  if alarmon:
    rpm  = Machine_Data["decoded_payload"]['rpm'] + toadd
  elif !brokenmachine:
    rpm = Mahcine_Data["decodec_payload"]['rpm'] + random.choice([-50,200])
  else:
    rpm = Machine_Data["decoded_payload"]['rpm'] + 100

    Machine_Date["decoded_payload"]['rpm'] = rpm





def generatenewdata():
  global Machine_Data
  rpm =               Machine_Data["decoded_payload"]["rpm"] + random.choice([-50,200])
  oilpressure =       Machine_Data["decoded_payload"]["oil_pressure"] +random.choice([-0.1,0.5])
  batterypontential = Machine_Data["decoded_payload"]["battery_pontential"] + random.choice([-0.1,0.2])
  consumption =       Machine_Data["decoded_payload"]["consumption"] + random.choice([-1,1])
  coolanttemp =       Machine_Data["decoded_payload"]["coolant_temperature"] + random.choice([-0.3,1.0])
    
  Machine_Data["decoded_payload"]["rpm"] = max(800,min(rpm,3000))
  Machine_Data["decoded_payload"]["oil_pressure"] = max(1.5,min(oilpressure,8.0))
  Machine_Data["decoded_payload"]["battery_pontential"] = max(10,min(batterypontential,14))
  Machine_Data["decoded_payload"]["consumption"] = max(1,min(consumption,50))
  Machine_Data["decoded_payload"]["coolant_temperature"] = max(70,min(coolanttemp,130))














