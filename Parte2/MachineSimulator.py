import sys
import time
import json
from random import choice
from datetime import datetime
import threading
import paho.mqtt.client as mqtt

# Config do ficheiro
GroupID = sys.argv[1]
MACHINE_UPDATE_TIME = sys.argv[2] # pode ser alterado
Machine_Code =sys.argv[3]
CodeMachine={"A23X":1,"B47Y":2,"C89Z":3,"D56W":4,"E34V":5,"F78T":6,"G92Q":7,"H65P":8}
machineID = CodeMachine[Machine_Code]

# TOPICOS
# para enviar
network_topic = f'v3/{GroupID}@ttn/devices/M{CodeMachine[Machine_Code]}/up'
# mensagens de controlo
DataManagerAgent_topic = f'v3/{GroupID}@ttn/devices/M{CodeMachine[Machine_Code]}/down/push_actuator'
# mensagens de alerta 
AlertManger_topic = f'v3/{GroupID}@ttn/devices/M{CodeMachine[Machine_Code]}/down/push_alert'


mqtthost = "10.6.1.9"
mqtthost = "broker.hivemq.com"
mqttport = 1883


# CONFIGS DA MAQUINA
TURNOFF = False
BROKEN = sys.argv[4].lower() == "true" if len(sys.argv) > 4 else False

unidades = {"oil_pressure":[1,0,1,0,1,0,1,0], # 0 means the value is in "normal" units
        "coolant_temperature":[0,0,0,0,1,1,1,1],
        "battery_potential":[0,0,0,0,0,0,0,1],
        "consumption":[0,1,1,0,1,0,0,1]
        }

# StartValues
OilPressureUnits = [3.2,46.4] #bar,psi
CoolantTempUnits = [92.0,197.6] #celsius,farenheit
BatteryPotentialUnits = [12.6,12600] #V,mV
ConsumptionUnits = [15.8,4.17] # l/h, gal/h

#ideal values
# NormalFlutuations
RPMSend = [-50,200]
OilPressureSend = {"0":[-0.1,0.5],"1":[-1.45,7.25]}
CoolantTempSend = [-0.3,1]
BatteryPotentialSend = {"0":[-0.1,0.2],"1":[-100,200]}
ConsumptionSend = {"0":[-1,1],"1":[-0.26,0.26]}

#AdjustingFlutuations
RPMadjust = 250
OilPressureadjust = {"0":0.5,"1":7.25}
CoolantTempadjust = 5
BatteryPotentialadjust = {"0":0.2,"1":200}
ConsumptionAdjust = {"0":1,"1":0.26}

#IdealVALUES 
RPMideal = 1100
oilpressureideal = [3,43.5]
batteryPotentialideal = [13,13000]
consumptionideal = [25,6.6]
coolanttempideal = [90,194] 

#CorrectingFlutuations
reducingorders = {"rpm":0,"oil_pressure":0,"coolant_temperature":0,"battery_potential":0,"consumption":0}

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
      "coolant_temperature": CoolantTempUnits[unidades['coolant_temperature'][machineID-1]], 
      "oil_pressure": OilPressureUnits[unidades["oil_pressure"][machineID-1]], 
      "battery_potential": BatteryPotentialUnits[unidades["battery_potential"][machineID-1]], 
      "consumption": ConsumptionUnits[unidades["consumption"][machineID-1]], 
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


def on_connect(client, userdata, flags, rc):
  if rc == 0:
    print("Connected to MQTT Broker!")
    client.subscribe(DataManagerAgent_topic)
    client.subscribe(AlertManger_topic)
  else:
    print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
  if msg.topic == DataManagerAgent_topic:
    print("CONTROLER")
    param,valor = processDMA(json.loads(msg.payload.decode()))
    if reducingorders[param]==0:
      if valor < 0:
        reducingorders[param] = -1
      else:
        reducingorders[param] = 1

  elif msg.topic == AlertManger_topic:
    print("ALERTA")
    procressAM(json.loads(msg.payload.decode()))

def processDMA(payload):
  param_map = {
            '01': 'rpm',
            '02': 'oil_pressure',
            '03': 'coolant_temperature',
            '04': 'battery_potential',
            '05': 'consumption'
            }
  encoded = payload["downlinks"][0]["frm_payload"]
  bytearr = encoded.split()
  parambit = bytearr[2][2:]
  param = param_map[parambit]
  value = bytearr[3]
  value_int = int(value, 16)
  if value_int > 127:
    value_int -= 256
  print(f"Received param: {param}, value: {value_int}")
  return param,value_int


def procressAM(payload):
  global TURNOFF
  TURNOFF = True

def showvalues():
  decoded = Machine_Data["uplink_message"]["decoded_payload"]
  print(f"RPM: {decoded['rpm']}")
  print(f"Oil Pressure: {decoded['oil_pressure']}")
  print(f"Coolant Temperature: {decoded['coolant_temperature']}")
  print(f"Battery Potential: {decoded['battery_potential']}")
  print(f"Consumption: {decoded['consumption']}")
  print(f"Machine Type: {decoded['machine_type']}")

def checktoreset():
  global Machine_Data
  unit_coolant = unidades["coolant_temperature"][machineID-1]
  coolant_temp = Machine_Data["uplink_message"]["decoded_payload"]["coolant_temperature"]

  rpm = Machine_Data["uplink_message"]["decoded_payload"]["rpm"]
  oil_pressure = Machine_Data["uplink_message"]["decoded_payload"]["oil_pressure"]
  battery_potential = Machine_Data["uplink_message"]["decoded_payload"]["battery_potential"]
  consumption = Machine_Data["uplink_message"]["decoded_payload"]["consumption"]

  if (unit_coolant == 0 and coolant_temp < 20) or (unit_coolant == 1 and coolant_temp < 68):
    if rpm == 0 and oil_pressure == 0 and battery_potential == 0 and consumption == 0:
      resetmachine()

def resetmachine():
  global Machine_Data, reducingorders, TURNOFF
  unit_oil = unidades["oil_pressure"][machineID-1]
  unit_battery = unidades["battery_potential"][machineID-1]
  unit_consumption = unidades["consumption"][machineID-1]
  unit_coolant = unidades["coolant_temperature"][machineID-1]

  Machine_Data["uplink_message"]["decoded_payload"]["rpm"] = RPMideal
  Machine_Data["uplink_message"]["decoded_payload"]["oil_pressure"] = oilpressureideal[unit_oil]
  Machine_Data["uplink_message"]["decoded_payload"]["battery_potential"] = batteryPotentialideal[unit_battery]
  Machine_Data["uplink_message"]["decoded_payload"]["consumption"] = consumptionideal[unit_consumption]
  Machine_Data["uplink_message"]["decoded_payload"]["coolant_temperature"] = coolanttempideal[unit_coolant]

  for k in reducingorders:
    reducingorders[k] = 0
  TURNOFF = False


def generateRPM():
    global Machine_Data
    if TURNOFF:
        rpm = 0
        Machine_Data["uplink_message"]["decoded_payload"]['rpm'] = 0
        return 

    elif reducingorders["rpm"] != 0:
        rpm = Machine_Data["uplink_message"]["decoded_payload"]['rpm'] + reducingorders["rpm"] * RPMadjust
        if (reducingorders["rpm"] == -1 and rpm <= RPMideal) or (reducingorders["rpm"] == 1 and rpm >= RPMideal):
            reducingorders["rpm"] = 0
    elif not BROKEN:
        rpm = Machine_Data["uplink_message"]["decoded_payload"]['rpm'] + choice(RPMSend)
    else:
        rpm = Machine_Data["uplink_message"]["decoded_payload"]['rpm'] + RPMadjust

    Machine_Data["uplink_message"]["decoded_payload"]['rpm'] = max(800, min(rpm, 3000))


def generateOilPressure():
  global Machine_Data
  unit_index = unidades["oil_pressure"][machineID-1]
  if TURNOFF:
    # diminuir till zero
    oil_pressure = 0
    Machine_Data["uplink_message"]["decoded_payload"]['oil_pressure'] = oil_pressure
    return

  elif reducingorders["oil_pressure"] != 0:
    oil_pressure = Machine_Data["uplink_message"]["decoded_payload"]['oil_pressure'] + reducingorders["oil_pressure"] * OilPressureadjust[str(unit_index)]
    if (reducingorders["oil_pressure"]==-1 and oil_pressure <= oilpressureideal[unit_index]) or (reducingorders["oil_pressure"]==1 and oil_pressure >= oilpressureideal[unit_index]):
      reducingorders["oil_pressure"] = 0
  elif not BROKEN:
    oil_pressure = Machine_Data["uplink_message"]["decoded_payload"]['oil_pressure'] + choice(OilPressureSend[str(unit_index)])
  else:
    oil_pressure = Machine_Data["uplink_message"]["decoded_payload"]['oil_pressure'] + OilPressureadjust[str(unit_index)]

  if unit_index == 0:
    oil_pressure = max(1.5, min(oil_pressure, 8.0))
  else:
    oil_pressure = max(20, min(oil_pressure, 120))

  Machine_Data["uplink_message"]["decoded_payload"]['oil_pressure'] = oil_pressure


def generatePotential():
  global Machine_Data
  unit_index = unidades["battery_potential"][machineID-1]

  if TURNOFF:
    # diminuir till zero
    potential = 0
    Machine_Data["uplink_message"]["decoded_payload"]['battery_potential'] = potential
    return

  elif reducingorders["battery_potential"] != 0:
    potential = Machine_Data["uplink_message"]["decoded_payload"]['battery_potential'] + reducingorders["battery_potential"] * BatteryPotentialadjust[str(unit_index)]
    if (reducingorders["battey_potential"]==-1 and potential <= batteryPotentialideal[unit_index]) or (reducingorders["battery_potential"]==1 and potential >= batteryPotentialideal[unit_index]):
      reducingorders["battery_potential"] = 0
  elif not BROKEN:
    potential = Machine_Data["uplink_message"]["decoded_payload"]['battery_potential'] + choice(BatteryPotentialSend[str(unit_index)])
  else:
    potential = Machine_Data["uplink_message"]["decoded_payload"]['battery_potential'] + BatteryPotentialadjust[str(unit_index)]

  if unit_index == 0:
    potential = max(10, min(potential, 14))
  else:
    potential = max(10000, min(potential, 14000))

  Machine_Data["uplink_message"]["decoded_payload"]['battery_potential'] = potential

def generateConsumption():
  global Machine_Data
  unit_index = unidades["consumption"][machineID-1]
  if TURNOFF:
    # diminuir till zero
    Machine_Data["uplink_message"]["decoded_payload"]['consumption'] = 0
    return
  elif reducingorders["consumption"] != 0:
    consumption = Machine_Data["uplink_message"]["decoded_payload"]['consumption'] + reducingorders["consumption"] * ConsumptionAdjust[str(unit_index)]
    if (reducingorders["consumption"]==-1 and consumption <= consumptionideal[unit_index]) or (reducingorders["consumption"]==1 and consumption >= consumptionideal[unit_index]):
      reducingorders["consumption"] = 0
  elif not BROKEN:
    consumption = Machine_Data["uplink_message"]["decoded_payload"]['consumption'] + choice(ConsumptionSend[str(unit_index)])
  else:
    consumption = Machine_Data["uplink_message"]["decoded_payload"]['consumption'] + ConsumptionAdjust[str(unit_index)]

  if unit_index == 0:
    consumption = max(1, min(consumption, 50))
  else:
    consumption = max(0.26, min(consumption, 13))

  Machine_Data["uplink_message"]["decoded_payload"]['consumption'] = consumption

def generateCoolantTemp():
  global Machine_Data
  unit_index = unidades["coolant_temperature"][machineID-1]
  if TURNOFF:
    # diminuir till zero
    temp = 0
    Machine_Data["uplink_message"]["decoded_payload"]['coolant_temperature'] = temp
    return
  elif reducingorders["coolant_temperature"] != 0:
    temp = Machine_Data["uplink_message"]["decoded_payload"]['coolant_temperature'] + reducingorders["coolant_temperature"] * CoolantTempadjust
    if (reducingorders["coolant_temperature"]==-1 and temp <= coolanttempideal[unit_index]) or (reducingorders["coolant_temperature"]==1 and temp >= coolanttempideal[unit_index]):
      reducingorders["coolant_temperature"] = 0
    
  elif not BROKEN:
    temp = Machine_Data["uplink_message"]["decoded_payload"]['coolant_temperature'] + choice(CoolantTempSend)
  else:
    temp = Machine_Data["uplink_message"]["decoded_payload"]['coolant_temperature'] + CoolantTempadjust

  if unit_index == 0:
    temp = max(70, min(temp, 130))
  else:
    temp = max(158, min(temp, 266))

  Machine_Data["uplink_message"]["decoded_payload"]['coolant_temperature'] = temp

def update_lorawan_conditions():
  meta = Machine_Data["uplink_message"]["rx_metadata"][0]
  meta["rssi"] += choice([-3, 0, 3])
  meta["rssi"] = max(-120, min(meta["rssi"], -50))
  meta["snr"] += choice([-0.5, 0, 0.5])
  meta["snr"] = max(-20, min(meta["snr"], 10))
  meta["channel_rssi"] += choice([-3, 0, 3])
  meta["channel_rssi"] = max(-120, min(meta["channel_rssi"], -50))

def generatenewdata2():
  generateRPM()
  generateOilPressure()
  generatePotential()
  generateConsumption()
  generateCoolantTemp()
  update_lorawan_conditions()

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(mqtthost,mqttport)
client.loop_start()

def monitor_turnoff_and_reset(delay_seconds=10): 
  def monitor():
    global TURNOFF, reducingorders
    while True:
      if TURNOFF:
        time.sleep(delay_seconds)
        if TURNOFF:
          resetmachine()
          for k in reducingorders:
            reducingorders[k] = 0
          TURNOFF = False 
      time.sleep(1)
  t = threading.Thread(target=monitor, daemon=True)
  t.start()

monitor_turnoff_and_reset(10) # 10 segundos

while True:

  generatenewdata2()
  client.publish(network_topic, json.dumps(Machine_Data, default=str))
  print(".")
  print(reducingorders)
  showvalues()
  time.sleep(float(MACHINE_UPDATE_TIME))
