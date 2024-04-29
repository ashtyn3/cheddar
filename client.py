import socket

HOST = "127.0.0.1"  # The server's hostname or IP address
PORT = 8080 # The port used by the server
SCRIPT = """
-- table("hi", {column(key, "id"), column(uint, "age")})
-- insert("hi", {kv("age", 3)})
drop_table("hi");
"""
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    data = bytes(SCRIPT+'\n', "utf-8")
    # while True:
    s.send(data)
