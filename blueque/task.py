class Task(object):
    def __init__(self, id, redis_task):
        self._id = id
        self._attributes = redis_task.get_task_data()

    @property
    def id(self):
        return self._id

    @property
    def status(self):
        return self._attributes["status"]

    @property
    def queue(self):
        return self._attributes["queue"]

    @property
    def parameters(self):
        return self._attributes["parameters"]

    @property
    def result(self):
        return self._attributes["result"]

    @property
    def error(self):
        return self._attributes["error"]

    @property
    def node(self):
        return self._attributes["node"]

    @property
    def pid(self):
        return self._attributes["pid"]

    @property
    def created(self):
        return self._attributes["created"]

    @property
    def updated(self):
        return self._attributes["updated"]
