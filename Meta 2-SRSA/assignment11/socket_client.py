import sys
import socket
import time

ClientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

print('Waiting for connection response')

if len(sys.argv) != 3:
    print(f"Usage: {sys.argv[0]} <host server> <port>")
    sys.exit(1)

host, port = sys.argv[1], int(sys.argv[2])

try:
    ClientSocket.connect((host, port))
    res = ClientSocket.recv(1024)
    print(res.decode('utf-8'))

    while True:
        try:
            Input = 'REQUEST'
            ClientSocket.send(Input.encode())
            res = ClientSocket.recv(1024)
            if not res:
                print("Server closed the connection.")
                break
            print(res.decode('utf-8'))
            time.sleep(2)
        except (BrokenPipeError, ConnectionResetError):
            print("Connection lost. Server might have closed the connection.")
            break

except socket.error as e:
    print(f"Socket error: {e}")
except KeyboardInterrupt:
    print("\nCaught keyboard interrupt, exiting.")
finally:
    ClientSocket.close()
    print("Connection closed.")
