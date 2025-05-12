# TCP Server
# Python 3 or above

import sys
import socket
import os
from _thread import *

threadCount = 0
clientNumber = 0

if len(sys.argv) != 3:
    print(f"Usage: {sys.argv[0]} <host> <port>")
    sys.exit(1)

host, port = sys.argv[1], int(sys.argv[2])
ServerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    ServerSocket.bind((host, port))
except socket.error as e:
    print(str(e))
    sys.exit(1)

print(f'Socket is listening in address {host} and port {port} ...')
ServerSocket.listen(5)

latest={}
def multi_threaded_client(connection, cli_n, temperatura):
    connection.send(str.encode('Server is working:'))
    while True:
        data = connection.recv(2048)

        req= data.decode('utf-8')
        if data:
            if req=='REQUEST':
                connection.send(str.encode(latest))
                print('Enviado')
        else:
            break


    print('Bye bye Client ' + str(cli_n) + '!')
    connection.close()
    return temp


try:
    temp=list()
    while True:
        Client, address = ServerSocket.accept()
        clientNumber +=1
        print('Connected to Client ' + str(clientNumber) + ', calling from: ' + address[0] + ':' + str(address[1]))
        start_new_thread(multi_threaded_client, (Client, clientNumber,temp))
        threadCount += 1
        print('Thread Number: ' + str(threadCount))

except KeyboardInterrupt:
    print("\nCaught keyboard interrupt, exiting")
finally:
    ServerSocket.close()
