import socket

HOST = "127.0.0.1"  # The server's hostname or IP address
PORT = 8080 # The port used by the server

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    data = bytearray([1,9, *bytes("key", "utf-8"), 0, *bytes("value", "utf-8")])
    s.sendall(data)
