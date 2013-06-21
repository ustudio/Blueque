from blueque.redis_task import RedisTask

import logging
import time
import uuid


class RedisQueue(object):
    def __init__(self, name, redis_client):
        self._name = name
        self._pending_name = self._key("pending_tasks", self._name)

        self._queues_key = self._key("queues")
        self._started_key = self._key("started_tasks", self._name)
        self._listeners_key = self._key("listeners", self._name)

        self._complete_key = self._key("complete_tasks", self._name)
        self._failed_key = self._key("failed_tasks", self._name)

        self._redis = redis_client

    def _running_job(self, node_id, pid, task_id):
        return " ".join((node_id, str(pid), task_id))

    def _key(self, *args):
        return '_'.join(("blueque",) + args)

    def _reserved_key(self, node_id):
        return self._key("reserved_tasks", self._name, node_id)

    def _log(self, message):
        logging.info("Blueque queue %s: %s" % (self._name, message))

    def add_listener(self, node_id):
        self._log("adding listener %s" % (node_id))
        with self._redis.pipeline() as pipeline:
            pipeline.sadd(self._listeners_key, node_id)
            pipeline.zincrby(self._queues_key, 1, self._name)
            pipeline.execute()

    def remove_listener(self, node_id):
        self._log("removing listener %s" % (node_id))
        with self._redis.pipeline() as pipeline:
            pipeline.zincrby(self._queues_key, -1, self._name)
            pipeline.srem(self._listeners_key, node_id)
            pipeline.execute()

    def enqueue(self, parameters):
        task_id = str(uuid.uuid4())

        self._log("adding task %s, parameters: %s" % (task_id, parameters))

        with self._redis.pipeline() as pipeline:
            now = time.time()
            pipeline.hmset(
                RedisTask.task_key(task_id),
                {
                    "status": "pending",
                    "queue": self._name,
                    "parameters": parameters,
                    "created": now,
                    "updated": now
                })

            pipeline.zincrby(self._key("queues"), 0, self._name)
            pipeline.lpush(self._pending_name, task_id)

            pipeline.execute()

        return task_id

    def dequeue(self, node_id):
        self._log("reserving task on %s" % (node_id))

        task_id = self._redis.rpoplpush(
            self._pending_name, self._reserved_key(node_id))

        if task_id is None:
            return None

        self._log("got task %s" % (task_id))

        self._redis.hmset(
            RedisTask.task_key(task_id),
            {
                "status": "reserved",
                "node": node_id,
                "updated": time.time()
            })

        return task_id

    def start(self, task_id, node_id, pid):
        self._log("starting task %s on %s, pid %i" % (task_id, node_id, pid))
        with self._redis.pipeline() as pipeline:
            pipeline.sadd(self._started_key, self._running_job(node_id, pid, task_id))
            pipeline.hmset(
                RedisTask.task_key(task_id),
                {"status": "started", "pid": pid, "updated": time.time()})

            pipeline.execute()

    def complete(self, task_id, node_id, pid, result):
        self._log(
            "completing task %s on %s, pid: %i, result: %s" % (task_id, node_id, pid, result))

        with self._redis.pipeline() as pipeline:
            pipeline.lrem(self._reserved_key(node_id), 1, task_id)
            pipeline.srem(self._started_key, self._running_job(node_id, pid, task_id))

            pipeline.hmset(
                RedisTask.task_key(task_id),
                {
                    "status": "complete",
                    "result": result,
                    "updated": time.time()
                })

            pipeline.lpush(self._complete_key, task_id)

            pipeline.execute()

    def fail(self, task_id, node_id, pid, error):
        self._log("failed task %s on %s, pid: %i, error: %s" % (task_id, node_id, pid, error))

        with self._redis.pipeline() as pipeline:
            pipeline.lrem(self._reserved_key(node_id), 1, task_id)
            pipeline.srem(self._started_key, self._running_job(node_id, pid, task_id))

            pipeline.hmset(
                RedisTask.task_key(task_id),
                {
                    "status": "failed",
                    "error": error,
                    "updated": time.time()
                })

            pipeline.lpush(self._failed_key, task_id)

            pipeline.execute()

    def delete_task(self, task_id, task_status):
        if task_status == "complete":
            finished_queue = self._complete_key
        elif task_status == "failed":
            finished_queue = self._failed_key
        else:
            raise ValueError("Cannot delete task with status %s" % (task_status))

        self._log("deleting task %s with status %s" % (task_id, task_status))

        with self._redis.pipeline() as pipeline:
            pipeline.delete(RedisTask.task_key(task_id))
            pipeline.lrem(finished_queue, 1, task_id)

            pipeline.execute()
