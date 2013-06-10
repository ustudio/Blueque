class Queue(object):
    def __init__(self, redis_queue):
        self._redis_queue = redis_queue

    def enqueue(self, parameters):
        return self._redis_queue.enqueue(parameters)
