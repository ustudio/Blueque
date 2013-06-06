import json
import redis
import uuid


class Queue(object):
    def __init__(self, name):
        self.name = name
        self.pending_name = self._key("pending_tasks", self.name)

        self._queues_key = self._key("queues")
        self._started_key = self._key("started_tasks", self.name)

        self.redis = redis.StrictRedis(host="", port=1234, db=0)

    def _running_job(self, node_id, pid, task_id):
        return " ".join((node_id, str(pid), task_id))

    def _key(self, *args):
        return '_'.join(("blueque",) + args)

    def _reserved_key(self, node_id):
        return self._key("reserved_tasks", self.name, node_id)

    def _task_key(self, task_id):
        return self._key("task", task_id)

    def add_listener(self):
        self.redis.zincrby(self._queues_key, 1, self.name)

    def remove_listener(self):
        self.redis.zincrby(self._queues_key, -1, self.name)

    def enqueue(self, parameters):
        task_id = uuid.uuid4()
        encoded_params = json.dumps(parameters)

        with self.redis.pipeline() as pipeline:
            pipeline.hmset(
                self._task_key(task_id),
                {
                    "status": "pending",
                    "queue": self.name,
                    "parameters": encoded_params
                })

            pipeline.zincrby(self._key("queues"), 0, self.name)
            pipeline.lpush(self.pending_name, task_id)

            pipeline.execute()

        return task_id

    def dequeue(self, node_id):
        task_id = self.redis.rpoplpush(
            self.pending_name, self._reserved_key(node_id))

        self.redis.hmset(self._task_key(task_id), {"status": "reserved", "node": "some_node"})

        return task_id

    def start(self, task_id, node_id, pid):
        with self.redis.pipeline() as pipeline:
            pipeline.sadd(self._started_key, self._running_job(node_id, pid, task_id))
            pipeline.hmset(self._task_key(task_id), {"status": "started", "pid": pid})
            pipeline.hget(self._task_key(task_id), "parameters")

            results = pipeline.execute()

            return json.loads(results[-1])

    def complete(self, task_id, node_id, pid, result):
        with self.redis.pipeline() as pipeline:
            pipeline.lrem(self._reserved_key(node_id), task_id)
            pipeline.srem(self._started_key, 1, self._running_job(node_id, pid, task_id))

            pipeline.hmset(
                self._task_key(task_id), {"status": "complete", "result": json.dumps(result)})

            pipeline.lpush(self._key("complete_tasks", self.name), task_id)

            pipeline.execute()

    def fail(self, task_id, node_id, pid, error):
        with self.redis.pipeline() as pipeline:
            pipeline.lrem(self._reserved_key(node_id), task_id)
            pipeline.srem(self._started_key, 1, self._running_job(node_id, pid, task_id))

            pipeline.hmset(
                self._task_key(task_id), {"status": "failed", "error": json.dumps(error)})

            pipeline.lpush(self._key("failed_tasks", self.name), task_id)

            pipeline.execute()
