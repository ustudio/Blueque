from blueque.redis_task import RedisTask
from blueque.task import Task


class TaskAdmin(Task):
    def __init__(self, redis, task_id):
        self._redis = redis
        self._redis_task = RedisTask(task_id, self._redis)
        super(TaskAdmin, self).__init__(task_id, self._redis_task)
