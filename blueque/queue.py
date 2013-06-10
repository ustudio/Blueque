class Queue(object):
    def __init__(self, redis_queue):
        self.redis_queue = redis_queue

    def enqueue(self, parameters):
        return self.redis_queue.enqueue(parameters)
