import redis


class Client(object):
    def __init__(self, hostname, port, db):
        self._redis = redis.StrictRedis(host=hostname, port=port, db=db)
