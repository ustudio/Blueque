from blueque.process_helpers import process_running

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
        processor.start(str(os.getpid()))

        try:
            result = self._task_callback(task)

            processor.complete(result)
        except Exception as e:
            processor.fail(str(e))
        finally:
            # _exit won't flush, so we need to, in case there are
            # error messages we want to see.
            logging.shutdown()
            sys.stdout.flush()
            sys.stderr.flush()

            os._exit(0)

    def _run_task(self, task):
        pid = self.fork_task(task)

        logging.info("Forked task %s to pid %i" % (task.id, pid))

        _, status = os.waitpid(pid, 0)

        logging.info(
            "Forked task %s exited with status %i" % (task.id, os.WEXITSTATUS(status)))

    def run(self):
        listener = self._client.get_listener(self._queue)

        task = listener.claim_orphan()

        while task is not None:
            if task.status == "reserved":
                self._run_task(task)
            elif task.status == "started":
                while process_running(int(task.pid)):
                    time.sleep(0.1)

            task = listener.claim_orphan()

        while True:
            task = listener.listen()

            self._run_task(task)
