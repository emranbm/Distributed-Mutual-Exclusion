import sys
import socket
import json
from typing import List
import time
import logging
import threading
from shared.communicator import Communicator
from shared.task import Task
from shared import message_types
from shared.readwritelock import ReadWriteLock
from shared.state import State

MASTER_PORT = None
NODES_COUNT = None
CURRENT_NODE_ID = None
CURRENT_NODE_INDEX = None
communicator = None

tasks: List[Task] = None
current_task: Task = None

LOG_LEVEL = logging.INFO

# Singhal algorithm arrays
SV: List[State] = None
SN: List[int] = None
TSV: List[State] = None
TSN: List[int] = None
lock = ReadWriteLock()
TOTAL_RESOURCES = None


def main():
    global MASTER_PORT
    global NODES_COUNT
    global TOTAL_RESOURCES
    global CURRENT_NODE_ID
    global CURRENT_NODE_INDEX
    MASTER_PORT = int(sys.argv[1])
    NODES_COUNT = int(sys.argv[2])
    TOTAL_RESOURCES = int(sys.argv[3])
    CURRENT_NODE_ID = int(sys.argv[4])
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
    lock.acquire_write()
    SV = [None for i in range(NODES_COUNT)]
    SN = [None for i in range(NODES_COUNT)]
    TSV = [None for i in range(NODES_COUNT)]
    TSN = [None for i in range(NODES_COUNT)]

    for j in range(CURRENT_NODE_ID - 1):
        SV[j] = State(current=0, need=TOTAL_RESOURCES)
    for j in range(CURRENT_NODE_ID - 1, NODES_COUNT):
        SV[j] = State(current=0, need=0)
    for j in range(NODES_COUNT):
        SN[j] = 0
        TSN[j] = 0
        TSV[j] = State(current=0, need=0)

    if CURRENT_NODE_ID == 1:
        SV[CURRENT_NODE_INDEX] = State(current=TOTAL_RESOURCES, need=0)
    logging.debug(f"SV: {SV}")
    lock.release_write()


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
    logging.debug(f"Received msg: {msg}")
    msg_type = msg["type"]
    if msg_type == message_types.TASKS:
        task_dicts = msg["data"]
        global tasks
        tasks = [Task(**d) for d in task_dicts]
        tasks.sort(key=lambda i: i.task_id, reverse=True)
        threading.Thread(target=trigger_a_task).start()
    elif msg_type == message_types.CS_REQUEST:
        logging.debug("Got CS request")
        node_id = msg['node_id']
        logging.debug(f"Got CS request from node {node_id}")
        node_index = node_id - 1
        sn = msg['data']['sn']
        node_current = msg['data']['current']
        node_need = msg['data']['need']
        lock.acquire_read()
        current_sn = SN[node_index]
        lock.release_read()
        if sn > current_sn:
            lock.acquire_write()
            SN[node_index] = sn
            lock.release_write()
            lock.acquire_read()
            current_state = SV[CURRENT_NODE_INDEX]
            lock.release_read()
            if current_state == State(current=0, need=0):
                lock.acquire_write()
                SV[node_index].need = node_need
                SV[node_index].current = node_current
                lock.release_write()
            elif current_state.extra < 0:
                lock.acquire_read()
                node_last_state = SV[node_index]
                lock.release_read()
                if node_last_state.extra >= 0:
                    lock.acquire_write()
                    SV[node_index].need = node_need
                    SV[node_index].current = node_current
                    count = current_state.current
                    SV[CURRENT_NODE_INDEX].current = 0
                    SV[node_index].current += count
                    lock.release_write()
                    log_master(f"Sending {count} to node {node_id}")
                    lock.acquire_read()
                    m = {
                        "type": message_types.TOKEN,
                        "node_id": CURRENT_NODE_ID,
                        "data": {
                            "count": count,
                            "TSV": [(s.current, s.need) for s in TSV],
                            "TSN": TSN
                        }
                    }
                    lock.release_read()
                    communicator.send(m, MASTER_PORT + node_id)
                    log_master(f"Requesting token from node {node_id}")
                    lock.acquire_read()
                    m = {
                        "type": message_types.CS_REQUEST,
                        "node_id": CURRENT_NODE_ID,
                        "data": {
                            "sn": SN[CURRENT_NODE_INDEX],
                            "current": SV[CURRENT_NODE_INDEX].current,
                            "need": SV[CURRENT_NODE_INDEX].need
                        }
                    }
                    lock.release_read()
                    communicator.send(m, MASTER_PORT + node_id)
            elif current_state.current == current_state.need:
                lock.acquire_write()
                SV[node_index].need = node_need
                SV[node_index].current = node_current
                lock.release_write()
            elif current_state.extra > 0:
                lock.acquire_write()
                extra_count = current_state.extra
                SV[node_index].current += extra_count
                TSV[node_index].current += extra_count
                TSN[node_index] = sn
                SV[CURRENT_NODE_INDEX].current -= extra_count
                lock.release_write()
                lock.acquire_read()
                m = {
                    "type": message_types.TOKEN,
                    "node_id": CURRENT_NODE_ID,
                    "data": {
                        "count": extra_count,
                        "TSV": [(s.current, s.need) for s in TSV],
                        "TSN": TSN
                    }
                }
                lock.release_read()
                communicator.send(m, MASTER_PORT + node_id)
            else:
                raise Exception(f"Unknown state: {current_state}")
    elif msg_type == message_types.TOKEN:
        node_id = msg['node_id']
        lock.acquire_write()
        count = msg['data']['count']
        TSV = [State(current=s[0], need=s[1]) for s in msg['data']['TSV']]
        TSN = msg['data']['TSN']
        log_master(f"Got {count} tokens from node {node_id}")
        TSV[CURRENT_NODE_INDEX].current += count
        SV[CURRENT_NODE_INDEX].current += count
        for j in range(NODES_COUNT):
            if TSN[j] > SN[j]:
                SV[j] = TSV[j]
        lock.release_write()
        try_entering_cs()
    else:
        raise Exception(f"Unknown message type: {msg_type}")


def run_task(task: Task):
    global current_task
    current_task = task
    try_entering_cs()


def try_entering_cs():
    log_master("Trying to enter critical section...")
    lock.acquire_write()
    SV[CURRENT_NODE_INDEX].need = current_task.resource_count
    current_state = SV[CURRENT_NODE_INDEX]
    lock.release_write()
    logging.debug(f"Current state: {current_state}")
    if current_state.extra >= 0:
        enter_cs()
        return
    lock.acquire_write()
    # SV[CURRENT_NODE_INDEX] = "R"
    SN[CURRENT_NODE_INDEX] += 1
    lock.release_write()
    lock.acquire_read()
    msg = {
        "type": message_types.CS_REQUEST,
        "node_id": CURRENT_NODE_ID,
        "data": {
            "sn": SN[CURRENT_NODE_INDEX],
            "current": SV[CURRENT_NODE_INDEX].current,
            "need": SV[CURRENT_NODE_INDEX].need,
        }
    }
    for j in range(NODES_COUNT):
        if j == CURRENT_NODE_INDEX:
            continue
        if SV[j].need > 0 or SV[j].current > 0:
            log_master(f"Requesting token from node {j + 1}")
            communicator.send(msg, MASTER_PORT + j + 1)
    lock.release_read()


def enter_cs():
    log_master(
        f"Entered Critical Section! For doing task '{current_task.task_id}'")
    # lock.acquire_write()
    # SV[CURRENT_NODE_INDEX] = "E"
    # lock.release_write()
    time.sleep(current_task.duration / 1000)
    exit_cs()


def exit_cs():
    log_master("Leaving Critical Section...")
    lock.acquire_write()
    count = SV[CURRENT_NODE_INDEX].current
    SV[CURRENT_NODE_INDEX].need = 0
    TSV[CURRENT_NODE_INDEX].need = 0
    for j in range(NODES_COUNT):
        if SN[j] > TSN[j]:
            TSV[j] = SV[j]
            TSN[j] = SN[j]
        else:
            SV[j] = TSV[j]
            SN[j] = TSN[j]
            
    needer_node_id = None
    for j in range(NODES_COUNT):
        if SV[j].extra < 0:
            needer_node_id = j + 1
            break
    logging.debug(f"needer_node_id = {needer_node_id}")
    if needer_node_id is None:
        SV[CURRENT_NODE_INDEX].current = count
        pass
    else:
        log_master(f"Sending {count} tokens to node {needer_node_id}")
        m = {
            "type": message_types.TOKEN,
            "node_id": CURRENT_NODE_ID,
            "data": {
                "count": count,
                "TSV": [(t.current, t.need) for t in TSV],
                "TSN": TSN
            }
        }
        communicator.send(m, MASTER_PORT + needer_node_id)
    lock.release_write()
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
