from blueque.admin.task_admin import TaskAdmin


class QueueAdmin(object):
    def __init__(self, redis, name, listeners):
        self._redis = redis
        self.name = name
        self.listeners = listeners

        self._queue_key = "blueque_all_tasks_" + self.name

    def get_tasks(self):
        task_ids = self._redis.smembers(self._queue_key)

        return map(lambda task_id: TaskAdmin(self._redis, task_id), task_ids)
