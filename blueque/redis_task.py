import logging
from collections import defaultdict


class RedisTask(object):
    _field_types = defaultdict(lambda: str, {
        "pid": int,
        "created": float,
        "updated": float
    })

    def __init__(self, id, redis):
        super(RedisTask, self).__init__()

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
        task_data = {}
        logging.info("get_task_data")
        logging.info("_field_types")
        logging.info(self._field_types)
        for field, value in self._redis.hgetall(self._task_key).items():
            logging.info("Field: {}, Value: {}, Field Type: {}".format(
                field, value, self._field_types[field](value)))
            task_data[field] = self._field_types[field](value)
        logging.info("task_data {}".format(task_data))
        return task_data
