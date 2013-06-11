class Listener(object):
    def __init__(self, name, queue):
        queue.add_listener(name)
