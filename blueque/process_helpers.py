import os


def process_running(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False
