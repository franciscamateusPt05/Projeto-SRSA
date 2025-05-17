import paho.mqtt.client as mqtt

# --- Configurações ---
GROUP_ID = 11
MQTT_BROKER = "10.6.1.9"  # Broker Mosquitto
MQTT_PORT = 1883
BASE_TOPIC = f"machine_{GROUP_ID}/#"

device_on = False

last_temperature = None
last_pressure = None
last_rpm = None


def check_sensor_health():
    global last_temperature, last_pressure, last_rpm

    if last_temperature is None or last_pressure is None or last_rpm is None:
        return "disconnected"

    # Flags de estado
    temp_valid = 10 <= last_temperature <= 200
    temp_healthy = 90 <= last_temperature <= 105

    pressure_valid = 0 <= last_pressure <= 8
    pressure_healthy = 1 <= last_pressure <= 5

    rpm_valid = 0 <= last_rpm <= 4000
    rpm_healthy = last_rpm <= 2500

    # Verifica se sensores estão fora de faixa
    if not temp_healthy and not pressure_healthy:
        return "danger"
    elif not temp_healthy or not pressure_healthy:
        return "problem"
    elif not rpm_healthy:
        return "rpm_warning"
    else:
        return "ok"


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

        else:
            # Para qualquer outro tipo de mensagem enviada pelo controller
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
            elif topic == f"machine_{GROUP_ID}/controller":
                print(payload)

            status = check_sensor_health()

            if status == "ok":
                print(
                    "Ok (Green LED ON) Temperature and pressure within healthy ranges"
                )
            elif status == "problem":
                print(
                    "Problem (Yellow LED ON) Temperature or pressure out of healthy ranges"
                )
            elif status == "danger":
                print(
                    "Danger (Red LED ON) Temperature and pressure outside healthy ranges"
                )
            elif status == "rpm_warning":
                print("RPM above limit (Buzzer ON) RPM out of healthy range")
            elif status == "disconnected":
                print("Machine sensors disconnected (Red LED Flashes and buzzer ON)")


        except ValueError:
            print("Erro ao converter os dados do sensor.")


# --- Configuração do Cliente MQTT ---
client = mqtt.Client()
# client.username_pw_set("srsa_sub", "srsa_password")
client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(MQTT_BROKER, MQTT_PORT)
    print("Aguardando comandos e dados...")
    client.loop_forever()
except KeyboardInterrupt:
    print("Desligando subscriber...")
    client.disconnect()
