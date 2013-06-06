import json
import redis
import uuid


class Queue(object):
    def __init__(self, name):
        self.name = name
        self.redis = redis.StrictRedis(host="", port=1234, db=0)

    def _running_job(self, node_id, pid, task_id):
        return " ".join((node_id, str(pid), task_id))

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

            pipeline.execute()

        return task_id

    def dequeue(self, node_id):
        task_id = self.redis.rpoplpush(self.name, node_id)

        self.redis.hmset(task_id, {"status": "reserved", "node": "some_node"})

        return task_id

    def start(self, task_id, node_id, pid):
        with self.redis.pipeline() as pipeline:
            pipeline.sadd("running_tasks", self._running_job(node_id, pid, task_id))
            pipeline.hmset(task_id, {"status": "started", "pid": pid})
            pipeline.hget(task_id, "parameters")

            results = pipeline.execute()

            return json.loads(results[-1])

    def complete(self, task_id, node_id, pid, result):
        with self.redis.pipeline() as pipeline:
            pipeline.lrem(node_id, task_id)
            pipeline.lrem("running_tasks", 1, self._running_job(node_id, pid, task_id))

            pipeline.hmset(task_id, {"status": "complete", "result": json.dumps(result)})

            pipeline.lpush("complete", task_id)

            pipeline.execute()

    def fail(self, task_id, node_id, pid, error):
        with self.redis.pipeline() as pipeline:
            pipeline.lrem(node_id, task_id)
            pipeline.lrem("running_tasks", 1, self._running_job(node_id, pid, task_id))

            pipeline.hmset(task_id, {"status": "failed", "error": json.dumps(error)})

            pipeline.lpush("failed", task_id)

            pipeline.execute()
