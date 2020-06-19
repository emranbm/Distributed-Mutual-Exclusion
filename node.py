import sys
import socket
import json
from typing import List
from shared.communicator import Communicator
from shared.task import Task
from shared import message_types
import time
import logging
import threading

MASTER_PORT = None
NODES_COUNT = None
CURRENT_NODE_ID = None
CURRENT_NODE_INDEX = None
communicator = None

tasks: List[Task] = None
current_task: Task = None

LOG_LEVEL = logging.INFO

# Singhal algorithm arrays
SV = None
SN = None
TSV = None
TSN = None


def main():
    global MASTER_PORT
    global NODES_COUNT
    global CURRENT_NODE_ID
    global CURRENT_NODE_INDEX
    MASTER_PORT = int(sys.argv[1])
    NODES_COUNT = int(sys.argv[2])
    CURRENT_NODE_ID = int(sys.argv[3])
    CURRENT_NODE_INDEX = CURRENT_NODE_ID - 1
    logging.basicConfig(level=LOG_LEVEL,
                        format=f'Node {CURRENT_NODE_ID}: ' + '%(levelname)s: %(message)s')
    global communicator
    communicator = Communicator(MASTER_PORT + CURRENT_NODE_ID, on_msg_received)
    initialize_arrays()
    request_getting_tasks()


def initialize_arrays():
    global SV
    global SN
    global TSV
    global TSN
    SV = [None for i in range(NODES_COUNT)]
    SN = [None for i in range(NODES_COUNT)]
    TSV = [None for i in range(NODES_COUNT)]
    TSN = [None for i in range(NODES_COUNT)]

    for j in range(CURRENT_NODE_ID - 1):
        SV[j] = "R"
    for j in range(CURRENT_NODE_ID, NODES_COUNT):
        SV[j] = "N"
    for j in range(NODES_COUNT):
        SN[j] = 0
        TSN[j] = 0
        TSV[j] = "N"

    if CURRENT_NODE_ID == 1:
        SV[CURRENT_NODE_INDEX] = "H"


def request_getting_tasks():
    message = {
        "id": 1,
        "type": message_types.GET_TASKS,
        "node_id": CURRENT_NODE_ID
    }
    communicator.send(message, MASTER_PORT)


def on_msg_received(msg, addr):
    global TSV
    global TSN
    msg_type = msg["type"]
    if msg_type == message_types.TASKS:
        task_dicts = msg["data"]
        global tasks
        tasks = [Task(**d) for d in task_dicts]
        tasks.sort(key=lambda i: i.task_id, reverse=True)
        threading.Thread(target=trigger_a_task).start()
    elif msg_type == message_types.CS_REQUEST:
        node_id = msg['node_id']
        logging.debug(f"Got CS request from node {node_id}")
        node_index = node_id - 1
        sn = msg['data']
        if sn > SN[node_index]:
            SN[node_index] = sn
            current_state = SV[CURRENT_NODE_INDEX]
            if current_state == "N":
                SV[node_index] = "R"
            elif current_state == "R":
                if SV[node_index] != "R":
                    SV[node_index] = "R"
                    log_master(f"Requestingfdfd token from node {node_id}")
                    m = {
                        "type": message_types.CS_REQUEST,
                        "node_id": CURRENT_NODE_ID,
                        "data": SN[CURRENT_NODE_INDEX]
                    }
                    communicator.send(m, MASTER_PORT + node_id)
            elif current_state == "E":
                SV[node_index] = "R"
            elif current_state == "H":
                SV[node_index] = "R"
                TSV[node_index] = "R"
                TSN[node_index] = sn
                SV[CURRENT_NODE_INDEX] = "N"
                m = {
                    "type": message_types.TOKEN,
                    "node_id": CURRENT_NODE_ID,
                    "data": {
                        "TSV": TSV,
                        "TSN": TSN
                    }
                }
                communicator.send(m, MASTER_PORT + node_id)
            else:
                raise Exception(f"Unknown state: {current_state}")
    elif msg_type == message_types.TOKEN:
        node_id = msg['node_id']
        TSV = msg['data']['TSV']
        TSN = msg['data']['TSN']
        log_master(f"Got token from node {node_id}")
        for j in range(NODES_COUNT):
            if TSV[j] == "R":
                SV[j] = "R"
        enter_cs()
    else:
        raise Exception(f"Unknown message type: {msg_type}")


def run_task(task: Task):
    global current_task
    current_task = task
    request_entering_cs()


def request_entering_cs():
    log_master("Trying to enter critical section...")
    if SV[CURRENT_NODE_INDEX] == "H":
        enter_cs()
    SV[CURRENT_NODE_INDEX] = "R"
    SN[CURRENT_NODE_INDEX] += 1
    msg = {
        "type": message_types.CS_REQUEST,
        "node_id": CURRENT_NODE_ID,
        "data": SN[CURRENT_NODE_INDEX]
    }
    for j in range(NODES_COUNT):
        if j == CURRENT_NODE_INDEX:
            continue
        if SV[j] == "R":
            log_master(f"Requesting token from node {j + 1}")
            communicator.send(msg, MASTER_PORT + j + 1)


def enter_cs():
    log_master(
        f"Entered Critical Section! For doing task '{current_task.task_id}'")
    SV[CURRENT_NODE_INDEX] = "E"
    time.sleep(current_task.duration / 1000)
    exit_cs()


def exit_cs():
    log_master("Leaving Critical Section...")
    SV[CURRENT_NODE_INDEX] = "N"
    TSV[CURRENT_NODE_INDEX] = "N"
    for j in range(NODES_COUNT):
        if SN[j] > TSN[j]:
            TSV[j] = SV[j]
            TSN[j] = SN[j]
        else:
            SV[j] = TSV[j]
            SN[j] = TSN[j]

    all_are_n = True
    for j in range(NODES_COUNT):
        if SV[j] != "N":
            all_are_n = False
            break
    logging.debug(f"all_are_n = {all_are_n}")
    if all_are_n:
        SV[CURRENT_NODE_INDEX] = "H"
    else:
        requester_node_id = None
        for j in range(NODES_COUNT):
            if SV[j] == "R":
                requester_node_id = j + 1
                break
        logging.debug(f"Sending token to node {requester_node_id}")
        m = {
            "type": message_types.TOKEN,
            "node_id": CURRENT_NODE_ID,
            "data": {
                "TSV": TSV,
                "TSN": TSN
            }
        }
        communicator.send(m, MASTER_PORT + requester_node_id)

    trigger_a_task()


def trigger_a_task():
    if len(tasks) == 0:
        return
    t = tasks.pop()
    run_task(t)


def log_master(msg: str):
    communicator.send({
        "type": message_types.REPORT,
        "node_id": CURRENT_NODE_ID,
        "data": msg
    }, MASTER_PORT)


if __name__ == "__main__":
    main()
