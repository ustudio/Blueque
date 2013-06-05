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
