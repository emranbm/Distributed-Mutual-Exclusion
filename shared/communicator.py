import socket
import threading
from typing import List
import json
from shared.task import Task


class Communicator:

    def __init__(self, port, on_recieved_func):
        threading.Thread(target=self._listen, args=(port, on_recieved_func)).start()

    def _listen(self, port, on_recieved_func):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("127.0.0.1", port))

        while True:
            data, addr = sock.recvfrom(1024)
            on_recieved_func(json.loads(data), addr)

    def send(self, msg, port, host="localhost"):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(bytes(json.dumps(msg),"utf-8"), (host, port))
        sock.close()
