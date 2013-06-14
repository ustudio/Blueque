import os
import threading


class ForkingRunner(threading.Thread):
    def __init__(self, client, queue, task_callback):
        self._client = client
        self._queue = queue
        self._task_callback = task_callback

    def fork_task(self, task):
        pid = os.fork()

        if pid > 0:
            return pid

        processor = self._client.get_processor(task)

        processor.start(os.getpid())

        try:
            result = self._task_callback(task.parameters)

            processor.complete(result)
        except Exception as e:
            processor.fail(str(e))
        finally:
            os._exit(0)

    def run(self):
        listener = self._client.get_listener(self._queue)

        while True:
            task = listener.listen()

            pid = self.fork_task(task)

            _, status = os.waitpid(pid, 0)
