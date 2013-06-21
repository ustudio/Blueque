import logging
import os
import random
import sys
import time


class ForkingRunner(object):
    def __init__(self, client, queue, task_callback):
        super(ForkingRunner, self).__init__()

        self._client = client
        self._queue = queue
        self._task_callback = task_callback

    def start(self):
        pid = os.fork()

        if pid > 0:
            self._watcher_pid = pid
            return

        try:
            self.run()
        finally:
            os._exit(0)

    def is_alive(self):
        return os.waitpid(self._watcher_pid, os.WNOHANG)[0] == 0

    def fork_task(self, task):
        pid = os.fork()

        if pid > 0:
            return pid

        logging.info("Process forked to run task %s" % (task.id))

        # Reseed the random number generator, since we inherited it
        # from our parent after the fork
        random.seed()

        os.setsid()

        logging.info("Getting Processor to run task %s" % (task.id))
        processor = self._client.get_processor(task)

        logging.info("Starting to run task %s" % (task.id))
        processor.start(os.getpid())

        try:
            result = self._task_callback(task)

            processor.complete(result)
        except Exception as e:
            processor.fail(str(e))
        finally:
            # _exit won't flush, so we need to, in case there are
            # error messages we want to see.
            sys.stdout.flush()
            sys.stderr.flush()

            os._exit(0)

    def run(self):
        listener = self._client.get_listener(self._queue)

        while True:
            task = listener.listen()

            pid = self.fork_task(task)

            logging.info("Forked task %s to pid %i" % (task.id, pid))

            _, status = os.waitpid(pid, 0)

            logging.info(
                "Forked task %s exited with status %i" % (task.id, os.WEXITSTATUS(status)))


def run(client, queue, task_callback, concurrency=1):
    threads = []

    while True:
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)

        while len(threads) < concurrency:
            thread = ForkingRunner(client, queue, task_callback)
            thread.start()

            threads.append(thread)

        time.sleep(1)
