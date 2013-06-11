class Listener(object):
    def __init__(self, name, queue):
        self._name = name
        self._queue = queue
        self._queue.add_listener(name)

    def listen(self, callback):
        callback(self._queue.dequeue(self._name))
