from blueque.redis_task import RedisTask

import mock
import unittest


class TestRedisTask(unittest.TestCase):
    @mock.patch("redis.StrictRedis", autospec=True)
    def setUp(self, redis_class):
        self.mock_redis = redis_class.return_value

        self.redis_task = RedisTask("some_task", self.mock_redis)

    def test_get_task_data(self):
        self.mock_redis.hgetall.return_value = {"status": "pending"}

        task_data = self.redis_task.get_task_data()

        self.mock_redis.hgetall.assert_called_with("blueque_task_some_task")

        self.assertEqual({"status": "pending"}, task_data)
