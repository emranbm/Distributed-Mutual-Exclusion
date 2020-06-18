class Task:
    def __init__(self, task_id, node_id, duration, resource_count):
        self.task_id = task_id
        self.node_id = node_id
        self.duration = duration
        self.resource_count = resource_count
        
    def to_dict(self):
        return {
            "task_id": self.task_id,
            "node_id": self.node_id,
            "duration": self.duration,
            "resource_count": self.resource_count,
        }