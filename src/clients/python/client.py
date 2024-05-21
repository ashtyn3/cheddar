import socket
from typing import List


class Client:
    def __init__(self, host: str, port: int) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.address = (host, port);
        self.socket.connect(self.address)

        
