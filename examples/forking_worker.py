# This hasn't actually been run; it's just a scratch-pad, for now, to
# figure out what API we want

from blueque import Client
from blueque import forking_runner

import time


def do_work(task):
    print task.id, task.parameters

    time.sleep(1000)

    return "result"


if __name__ == "__main__":
    client = Client(hostname="localhost", port=6379, db=0)

    forking_runner.run(client, "some.queue", do_work, 4)
