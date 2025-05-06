import paho.mqtt.client as mqtt
import RPi.GPIO as GPIO
import time

# --- Configurações ---
GROUP_ID = 11
MQTT_BROKER = "10.6.1.71"
MQTT_PORT = 1883
BASE_TOPIC = f"machine_{GROUP_ID}/#"

device_on = False
last_temperature = None
last_pressure = None
last_rpm = None

# --- GPIO Setup ---
LED_GREEN = 26
LED_YELLOW = 6
LED_RED = 22
BUZZER = 12

GPIO.setmode(GPIO.BCM)
GPIO.setup(LED_GREEN, GPIO.OUT)
GPIO.setup(LED_YELLOW, GPIO.OUT)
GPIO.setup(LED_RED, GPIO.OUT)
GPIO.setup(BUZZER, GPIO.OUT)


def reset_outputs():
    GPIO.output(LED_GREEN, False)
    GPIO.output(LED_YELLOW, False)
    GPIO.output(LED_RED, False)
    GPIO.output(BUZZER, False)


def check_sensor_health():
    global last_temperature, last_pressure, last_rpm

    if last_temperature is None or last_pressure is None or last_rpm is None:
        return "disconnected"

    temp_healthy = 90 <= last_temperature <= 105
    pressure_healthy = 1 <= last_pressure <= 5
    rpm_healthy = last_rpm <= 2500

    if not temp_healthy and not pressure_healthy:
        return "danger"
    elif not temp_healthy or not pressure_healthy:
        return "problem"
    elif not rpm_healthy:
        return "rpm_warning"
    else:
        return "ok"


def update_outputs(status):
    reset_outputs()

    if status == "ok":
        GPIO.output(LED_GREEN, True)
        print("Ok (Green LED ON) Temperature and pressure within healthy ranges")
    elif status == "problem":
        GPIO.output(LED_YELLOW, True)
        print("Problem (Yellow LED ON) Temperature or pressure out of healthy ranges")
    elif status == "danger":
        GPIO.output(LED_RED, True)
        print("Danger (Red LED ON) Temperature and pressure outside healthy ranges")
    elif status == "rpm_warning":
        GPIO.output(BUZZER, True)
        print("RPM above limit (Buzzer ON) RPM out of healthy range")
    elif status == "disconnected":
        GPIO.output(LED_RED, True)
        GPIO.output(BUZZER, True)
        print("Machine sensors disconnected (Red LED Flashes and buzzer ON)")


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Conectado ao Broker MQTT!")
        client.subscribe(BASE_TOPIC)


def on_message(client, userdata, msg):
    global device_on, last_temperature, last_pressure, last_rpm

    topic = msg.topic
    payload = msg.payload.decode().strip()

    if topic == f"machine_{GROUP_ID}/controller":
        if payload == "1":
            device_on = True
            print("Dispositivo LIGADO")
        elif payload == "0":
            device_on = False
            print("Dispositivo DESLIGADO")
            reset_outputs()
        else:
            reset_outputs()
            GPIO.output(LED_RED, True)
            GPIO.output(BUZZER, True)
            print(payload)
        return

    if device_on:
        try:
            if topic == f"machine_{GROUP_ID}/coolant":
                last_temperature = float(payload)
            elif topic == f"machine_{GROUP_ID}/pressure":
                last_pressure = float(payload)
            elif topic == f"machine_{GROUP_ID}/rpm":
                last_rpm = float(payload)

            status = check_sensor_health()
            update_outputs(status)

        except ValueError:
            print("Erro ao converter os dados do sensor.")


# --- MQTT Client ---
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(MQTT_BROKER, MQTT_PORT)
    print("Aguardando comandos e dados...")
    client.loop_forever()
except KeyboardInterrupt:
    print("Desligando subscriber...")
    client.disconnect()
    reset_outputs()
    GPIO.cleanup()
