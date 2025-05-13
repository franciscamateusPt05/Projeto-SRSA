import sys
import socket
from _thread import *
import paho.mqtt.client as mqtt
import json

threadCount = 0
clientNumber = 0
latest_data = {"Temperature": "-", "Humidity": "-", "Motion": "-"}

# MQTT settings
MQTT_BROKER = "10.6.1.9"
MQTT_PORT = 1883
MQTT_TOPIC = f"Group{sys.argv[3]}/sensor_data"  # GroupID passado como 3º argumento

# MQTT Callback
def on_message(client, userdata, msg):
    global latest_data
    try:
        latest_data = json.loads(msg.payload.decode())
        print(f"[MQTT] Recebido: {latest_data}")
    except json.JSONDecodeError:
        print("[MQTT] Erro ao decodificar JSON")

mqtt_client = mqtt.Client()
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
mqtt_client.subscribe(MQTT_TOPIC)
mqtt_client.loop_start()

# Verificação de argumentos
if len(sys.argv) != 4:
    print(f"Usage: {sys.argv[0]} <host> <port> <GroupID>")
    sys.exit(1)

host, port = sys.argv[1], int(sys.argv[2])

ServerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    ServerSocket.bind((host, port))
except socket.error as e:
    print(str(e))
    sys.exit(1)

print(f'[TCP SERVER] A escutar em {host}:{port} no tópico {MQTT_TOPIC}...')
ServerSocket.listen(5)

# Função para cada cliente
def multi_threaded_client(connection, cli_n):
    connection.send(str.encode('Conectado ao Servidor. Escreve "REQUEST" para obter os dados do sensor.\n'))
    while True:
        try:
            data = connection.recv(2048)
            req = data.decode('utf-8').strip()
            if not data:
                break

            if req.upper() == 'REQUEST':
                message = f"Temperatura: {latest_data['Temperature']}°C | Humidade: {latest_data['Humidity']}% | Movimento: {latest_data['Motion']}\n"
                connection.sendall(message.encode())
                print(f"[TCP] Enviado para Cliente {cli_n}: {message.strip()}")
            else:
                connection.sendall("Comando inválido. Usa 'REQUEST'.\n")
        except:
            break

    print(f'Cliente {cli_n} desconectado.')
    connection.close()

# Ciclo principal
try:
    while True:
        Client, address = ServerSocket.accept()
        clientNumber += 1
        print(f'Cliente {clientNumber} ligado: {address[0]}:{address[1]}')
        start_new_thread(multi_threaded_client, (Client, clientNumber))
        threadCount += 1
        print(f'Total de threads: {threadCount}')
except KeyboardInterrupt:
    print("\nServidor interrompido.")
finally:
    ServerSocket.close()
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
