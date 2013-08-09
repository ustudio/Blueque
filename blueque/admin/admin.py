from blueque.admin.queue_admin import QueueAdmin


class Admin(object):
    def __init__(self, redis):
        self._redis = redis

    def get_queues(self):
        queue_ranks = self._redis.zrange("blueque_queues", 0, -1, withscores=True)

        return dict(
            map(lambda (name, rank): (name, QueueAdmin(name, rank)), queue_ranks)
        )
