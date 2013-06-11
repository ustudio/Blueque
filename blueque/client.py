from blueque.queue import Queue
from blueque.redis_queue import RedisQueue
from blueque.redis_task import RedisTask
from blueque.task import Task

import redis


class Client(object):
    def __init__(self, hostname, port, db):
        self._redis = redis.StrictRedis(host=hostname, port=port, db=db)

    def get_queue(self, name):
        redis_queue = RedisQueue(name, self._redis)
        return Queue(redis_queue)

    def get_task(self, task_id):
        redis_task = RedisTask(task_id, self._redis)
        return Task(task_id, redis_task)
