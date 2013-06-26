class Queue(object):
    def __init__(self, name, redis_queue):
        self._name = name
        self._redis_queue = redis_queue

    def enqueue(self, parameters):
        return self._redis_queue.enqueue(parameters)

    def schedule(self, parameters, eta):
        return self._redis_queue.schedule(parameters, eta)

    def delete_task(self, task):
        if task.queue != self._name:
            raise ValueError("Task %s is not in queue %s" % (task.id, self._name))

        self._redis_queue.delete_task(task.id, task.status)
