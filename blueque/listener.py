import time


class Listener(object):
    def __init__(self, name, queue):
        self._name = name
        self._queue = queue
        self._queue.add_listener(name)

    def listen(self, callback):
        while True:
            task_id = self._queue.dequeue(self._name)
            if task_id is not None:
                callback(task_id)
            else:
                time.sleep(1)
