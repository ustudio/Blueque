from blueque.listener import Listener
from blueque.processor import Processor
from blueque.queue import Queue
from blueque.redis_queue import RedisQueue
from blueque.redis_task import RedisTask
from blueque.task import Task

import os
import redis
import socket


class Client(object):
    def __init__(self, hostname, port, db):
        super(Client, self).__init__()

        self._redis = redis.StrictRedis(host=hostname, port=port, db=db)

    def get_queue(self, name):
        redis_queue = RedisQueue(name, self._redis)
        return Queue(name, redis_queue)

    def get_task(self, task_id):
        redis_task = RedisTask(task_id, self._redis)
        return Task(task_id, redis_task)

    def get_listener(self, queue_name):
        listener_name = "_".join((socket.getfqdn(), str(os.getpid())))
        redis_queue = RedisQueue(queue_name, self._redis)
        return Listener(listener_name, redis_queue, self.get_task)

    def get_processor(self, task):
        redis_queue = RedisQueue(task.queue, self._redis)

        return Processor(task, redis_queue)
