class RedisTask(object):
    def __init__(self, id, redis):
        self._id = id
        self._redis = redis
        self._task_key = self.task_key(self._id)

    @staticmethod
    def task_key(task_id):
        return RedisTask._key("task", task_id)

    @staticmethod
    def _key(*args):
        return '_'.join(("blueque",) + args)

    def get_task_data(self):
        return self._redis.hgetall(self._task_key)
