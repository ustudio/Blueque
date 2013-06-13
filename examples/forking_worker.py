# This hasn't actually been run; it's just a scratch-pad, for now, to
# figure out what API we want

from blueque import Client

import json
import os
import threading
import time


def do_work():
    time.sleep(1000)


class WorkerThread(threading.Thread):
    def __init__(self, client):
        self._client = client

    def fork_task(self, task):
        pid = os.fork()

        if pid > 0:
            return pid

        processor = self._client.get_processor(task)

        processor.start(os.getpid())

        try:
            result = do_work(json.loads(task.parameters))
            processor.complete(json.dumps(result))
        except Exception as e:
            processor.fail(str(e))
        finally:
            os._exit(0)

    def run(self):
        listener = self._client.get_listener()

        while True:
            task = listener.listen()

            pid = self.fork_task(task)

            pid, result = os.waitpid(pid, 0)
            # should check result and clean up if process died abnormally.


if __name__ == "__main__":
    concurrency = 4
    threads = []

    client = Client()

    while True:
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)

        while len(threads) < concurrency:
            thread = WorkerThread(client)
            thread.start()
            # There is a race condition between here and checking if the
            # thread is alive when the outer while loop wraps around.
            threads.append(thread)
