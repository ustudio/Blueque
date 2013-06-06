import json
import redis
import uuid


class Queue(object):
    def __init__(self, name):
        self.name = name
        self.redis = redis.StrictRedis(host="", port=1234, db=0)

    def enqueue(self, parameters):
        task_id = uuid.uuid4()
        encoded_params = json.dumps(parameters)

        with self.redis.pipeline() as pipeline:
            pipeline.hmset(
                task_id,
                {
                    "status": "pending",
                    "queue": self.name,
                    "parameters": encoded_params
                })

            pipeline.lpush(self.name, task_id)

        return task_id

    def dequeue(self, node_id):
        task_id = self.redis.rpoplpush(self.name, node_id)

        self.redis.hmset(task_id, {"status": "reserved", "node": "some_node"})

        return task_id

    def start(self, task_id, node_id, pid):
        with self.redis.pipeline() as pipeline:
            pipeline.sadd("running_tasks", " ".join((node_id, str(pid), task_id)))

            pipeline.hmset(task_id, {"status": "started", "pid": pid})
