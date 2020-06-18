import socket
import subprocess
import logging
import sys
from shared.communicator import Communicator
from shared.task import Task
from shared import message_types


MASTER_PORT = 1234
LOG_LEVEL = logging.DEBUG

communicator = None
tasks = None
node_ids = None


def main():
    logging.basicConfig(level=LOG_LEVEL,
                        format='%(levelname)s: %(message)s')
    logging.debug("Reading tasks...")
    global tasks
    tasks = read_tasks()
    global communicator
    logging.debug("Starting communicator...")
    communicator = Communicator(MASTER_PORT, on_msg_recieved)
    global node_ids
    node_ids = set([t.node_id for t in tasks])
    logging.debug(f"Starting nodes: {node_ids}")
    start_nodes(node_ids)


def read_tasks():
    with open('tasks.txt', 'r') as f:
        lines = f.readlines()
    tasks = []
    for l in lines:
        props = l.split(':')
        tasks.append(Task(*props))
    return tasks


def start_nodes(node_ids):
    for node_id in node_ids:
        subprocess.Popen([sys.executable,
                          "node.py",
                          str(MASTER_PORT),
                          str(len(node_ids)),
                          str(node_id)],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)


def on_msg_recieved(msg, addr):
    logging.debug(f"Received from: {addr}\n"
                  f"--msg: {msg}")
    (src_host, src_port) = addr
    msg_type = msg["type"]
    if msg_type == message_types.GET_TASKS:
        node_id = msg["data"]["node_id"]
        resp = {
            "id": msg["id"],
            "type": message_types.TASKS,
            "data": [t.to_dict() for t in tasks if t.node_id == node_id]
        }
    elif msg_type == message_types.REPORT:
        data = msg['data']
        node_id = data['node_id']
        txt = data['txt']
        logging.info(f"Report from {node_id}: {txt}")
    communicator.send(resp, src_port, src_host)


if __name__ == "__main__":
    main()
