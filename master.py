import socket
import subprocess
import logging
import sys
import atexit
import os
import signal
from shared.communicator import Communicator
from shared.task import Task
from shared import message_types

MASTER_PORT = 1234
LOG_LEVEL = logging.INFO
TOTAL_RESOURCES = None

communicator = None
tasks = None
node_ids = None
done_tasks = 0

node_pids = []


def main():
    logging.basicConfig(level=LOG_LEVEL,
                        format='%(levelname)s: %(message)s')
    
    if len(sys.argv) != 2:
        logging.error(f"Usage: python {sys.argv[0]} TOTAL_RESOURCES_COUNT")
        exit(1)
    
    global TOTAL_RESOURCES
    TOTAL_RESOURCES = int(sys.argv[1])
    logging.debug("Reading tasks...")
    global tasks
    tasks = read_tasks()
    logging.debug(f"Read tasks: {tasks}")
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
        proc = subprocess.Popen([sys.executable,
                                 "node.py",
                                 str(MASTER_PORT),
                                 str(len(node_ids)),
                                 str(TOTAL_RESOURCES),
                                 str(node_id)],)
        node_pids.append(proc.pid)


def on_msg_recieved(msg, addr):
    logging.debug(f"Received from: {addr}\n"
                  f"--msg: {msg}")
    (src_host, src_port) = addr
    msg_type = msg["type"]
    node_id = msg["node_id"]
    if msg_type == message_types.GET_TASKS:
        node_tasks = [t.to_dict() for t in tasks if t.node_id == node_id]
        logging.debug(f"Sending tasks to node {node_id}: {node_tasks}")
        resp = {
            "id": msg["id"],
            "type": message_types.TASKS,
            "data": node_tasks
        }
        communicator.send(resp, MASTER_PORT + node_id)
    elif msg_type == message_types.REPORT:
        txt = msg['data']
        logging.info(f"Report from {node_id}: {txt}")
        if txt == "Leaving Critical Section...":
            global done_tasks
            done_tasks += 1
            if done_tasks == len(tasks):
                logging.info("FINISHED!")
                exit(0)


def on_exit():
    for pid in node_pids:
        os.kill(pid, signal.SIGTERM)


atexit.register(on_exit)

if __name__ == "__main__":
    main()
