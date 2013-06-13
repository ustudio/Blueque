import time


class Listener(object):
    def __init__(self, name, queue, task_factory):
        self._name = name
        self._queue = queue
        self._queue.add_listener(name)
        self._task_factory = task_factory

    def listen(self):
        while True:
            task_id = self._queue.dequeue(self._name)
            if task_id is not None:
                return self._task_factory(task_id)
            else:
                time.sleep(1)
