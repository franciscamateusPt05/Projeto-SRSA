import paho.mqtt.client
import socket
import threading
import json


topic = f'{GroupID}/sensor_data'
broker = '10.6.1.9'
broket_port = 1883

# Create a tpc socket and allow multiple users to connect 

mqtt_client = mqtt.Client()
mqtt_client.connect(broker,broker_port)
mqtt_client.loop_start()

latest_data = {}
requestingclients = set()
clients_lock = threading.Lock()

def on_connect(client,userdata,flags,rc):
    print("Server is connected to mqtt")
    client.subscribe()
    # yo

def on_message(clietn,userdata,msg):
    global latest_data
    latest_data = json.loads(msg.payload.decode('utf-8'))

mqtt_client.on_connect = onconnect
mqtt_client.on_message = on_message


mqtt_client.connect(broker,broker_port)
mqtt_client.loop_start()


def handleClient_connections(client_socket):

    while True:
        try:
            data = client_socket.recv(1024)
            if not data:
                    break

            message = data.decode('utf-8')
            if message == "REQUEST":

            if latest_data:
                    client_socket.send(json.dumps(latest_data).encode('utf-8'))
                else:
                    client_socket.send(b'No data available')
        except Exception as e:
            print(f'Error {e}')
            break
    client_socket.close()


def start_server(host,port):
    server_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    server_socket.bind((host,port))
    server_socket.listen(5)

    while True:
        client_socket,addr = server_socket.accept()
        print(f'Accepted Connection from {addr}')
        client_handler = threading.Thread(target=handleClient_connections,args=(client_socket,))
        client_handler.start()

if __name__ == "__main__":
    start_server()
