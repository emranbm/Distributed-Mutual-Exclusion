import sys
import socket
import json
from typing import List
from shared.communicator import Communicator
from shared.task import Task
from shared import message_types
import time

MASTER_PORT = None
NODES_COUNT = None
NODE_ID = None
communicator = None


def main():
    global MASTER_PORT
    global NODES_COUNT
    global NODE_ID
    MASTER_PORT = int(sys.argv[1])
    NODES_COUNT = int(sys.argv[2])
    NODE_ID = int(sys.argv[3])
    global communicator
    communicator = Communicator(MASTER_PORT + NODE_ID, on_msg_received)
    request_getting_tasks()


def request_getting_tasks():
    message = {
        "id": 1,
        "type": message_types.GET_TASKS,
        "data": {
            "node_id": NODE_ID
        }
    }
    communicator.send(message, MASTER_PORT)


def on_msg_received(msg, addr):
    if msg["type"] == message_types.TASKS:
        task_dicts = msg["data"]
        tasks = [Task(**d) for d in task_dicts]
        do_tasks(tasks)


def do_tasks(tasks: List[Task]):
    for task in tasks:
        run_task(task)
        
def run_task(task: Task):
    log_master(f"Trying to do task {task.task_id} with {task.resource_count} resources...")

def log_master(msg: str):
    communicator.send({
        "type": message_types.REPORT,
        "data": {
            "node_id": NODE_ID,
            "txt": msg
        }
    }, MASTER_PORT)

if __name__ == "__main__":
    main()
