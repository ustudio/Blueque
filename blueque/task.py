class Task(object):
    def __init__(self, id, redis_task):
        super(Task, self).__init__()

        self._id = id
        self._attributes = redis_task.get_task_data()

    @property
    def id(self):
        return self._id

    @property
    def status(self):
        return self._attributes.get("status")

    @property
    def queue(self):
        return self._attributes.get("queue")

    @property
    def parameters(self):
        return self._attributes.get("parameters")

    @property
    def result(self):
        return self._attributes.get("result")

    @property
    def error(self):
        return self._attributes.get("error")

    @property
    def node(self):
        return self._attributes.get("node")

    @property
    def pid(self):
        return self._attributes.get("pid")

    @property
    def created(self):
        return self._attributes.get("created")

    @property
    def updated(self):
        return self._attributes.get("updated")
