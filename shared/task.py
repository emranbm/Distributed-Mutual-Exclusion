class Task:
    def __init__(self, task_id, node_id, duration, resource_count):
        self.task_id = int(task_id)
        self.node_id = int(node_id)
        self.duration = int(duration)
        self.resource_count = int(resource_count)
        
    def to_dict(self):
        return {
            "task_id": self.task_id,
            "node_id": self.node_id,
            "duration": self.duration,
            "resource_count": self.resource_count,
        }
        
    def __repr__(self):
        import json
        return json.dumps(self.to_dict())