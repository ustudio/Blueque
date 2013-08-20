from blueque.process_helpers import process_running

import os
import socket
import time


class Listener(object):
    def __init__(self, queue, task_factory):
        super(Listener, self).__init__()

        self._hostname = socket.getfqdn()
        self._pid = os.getpid()
        self._name = "_".join((self._hostname, str(self._pid)))
        self._queue = queue
        self._queue.add_listener(self._name)
        self._task_factory = task_factory

    def _parse_name(self, name):
        host, pid = name.rsplit('_', 1)

        return host, int(pid)

    def listen(self):
        while True:
            task_id = self._queue.dequeue(self._name)
            if task_id is not None:
                return self._task_factory(task_id)
            else:
                time.sleep(1)

    def claim_orphan(self):
        for listener in self._queue.get_listeners():
            host, pid = self._parse_name(listener)

            if host != self._hostname:
                continue

            if pid == self._pid:
                continue

            if process_running(pid):
                continue

            if self._queue.remove_listener(listener) == 0:
                # already claimed
                continue

            task_id = self._queue.reclaim_task(listener, self._name)
            if task_id is None:
                continue

            return self._task_factory(task_id)

        return None
