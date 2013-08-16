from blueque.redis_task import RedisTask

import logging
import time
import uuid


class RedisQueue(object):
    def __init__(self, name, redis_client):
        super(RedisQueue, self).__init__()

        self._name = name
        self._pending_name = self._key("pending_tasks", self._name)
        self._scheduled_key = self._key("scheduled_tasks", self._name)

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

    def _generate_task_id(self):
        return str(uuid.uuid4())

    def _log(self, message):
        logging.info("Blueque queue %s: %s" % (self._name, message))

    def add_listener(self, node_id):
        self._log("adding listener %s" % (node_id))
        with self._redis.pipeline() as pipeline:
            pipeline.sadd(self._listeners_key, node_id)
            pipeline.zincrby(self._queues_key, self._name, amount=1)
            pipeline.execute()

    def remove_listener(self, node_id):
        self._log("removing listener %s" % (node_id))

        removed = self._redis.srem(self._listeners_key, node_id)

        if removed > 0:
            self._log("removed listener")
            self._redis.zincrby(self._queues_key, self._name, amount=-1)

        return removed

    def get_listeners(self):
        return self._redis.smembers(self._listeners_key)

    def _generate_task(self, pipeline, status, parameters, **kwargs):
        task_id = self._generate_task_id()

        self._log("adding %s task %s, parameters: %s" % (status, task_id, parameters))

        now = time.time()

        task_data = {
            "status": status,
            "queue": self._name,
            "parameters": parameters,
            "created": now,
            "updated": now
        }

        task_data.update(kwargs)

        pipeline.hmset(RedisTask.task_key(task_id), task_data)

        pipeline.zincrby(self._key("queues"), self._name, amount=0)

        return task_id

    def schedule(self, parameters, eta):
        if eta < time.time():
            return self.enqueue(parameters)

        with self._redis.pipeline() as pipeline:
            task_id = self._generate_task(pipeline, "scheduled", parameters, eta=eta)

            pipeline.zadd(self._scheduled_key, eta, task_id)

            pipeline.execute()

        return task_id

    def enqueue(self, parameters):
        with self._redis.pipeline() as pipeline:
            task_id = self._generate_task(pipeline, "pending", parameters)

            pipeline.lpush(self._pending_name, task_id)

            pipeline.execute()

        return task_id

    def enqueue_due_tasks(self):
        def enqueue_transaction(pipeline):
            now = time.time()
            due_tasks = pipeline.zrangebyscore(self._scheduled_key, 0, now)

            if len(due_tasks) == 0:
                self._log("no due tasks")
                return

            self._log("enqueuing due tasks: %s" % (due_tasks))

            pipeline.multi()

            pipeline.zremrangebyscore(self._scheduled_key, 0, now)

            for task in due_tasks:
                pipeline.lpush(self._pending_name, task)
                pipeline.hmset(RedisTask.task_key(task), {"status": "pending", "updated": now})

        self._redis.transaction(enqueue_transaction, self._scheduled_key)

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

    def reclaim_task(self, old_node, new_node):
        task_id = self._redis.lindex(self._reserved_key(old_node), 0)

        if task_id is not None:
            self._redis.hset(RedisTask.task_key(task_id), "reclaimed_node", new_node)

        return task_id

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
