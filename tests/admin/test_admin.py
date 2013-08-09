import mock
import redis
import unittest

from blueque.admin.admin import Admin


class TestAdmin(unittest.TestCase):
    def setUp(self):
        self.mock_redis = mock.MagicMock(spec=redis.StrictRedis)
        self.admin = Admin(self.mock_redis)

    def test_returns_nothing_with_no_queues(self):
        self.mock_redis.zrange.return_value = []

        self.assertEqual({}, self.admin.get_queues())
        self.mock_redis.zrange.assert_called_with("blueque_queues", 0, -1, withscores=True)

    def test_returns_two_queues_when_present(self):
        self.mock_redis.zrange.return_value = [("some_queue", 2), ("other_queue", 1)]

        queues = self.admin.get_queues()

        self.mock_redis.zrange.assert_called_with("blueque_queues", 0, -1, withscores=True)

        self.assertItemsEqual(["some_queue", "other_queue"], queues.keys())
        self.assertEqual("some_queue", queues["some_queue"].name)
        self.assertEqual(2, queues["some_queue"].listeners)

        self.assertEqual("other_queue", queues["other_queue"].name)
        self.assertEqual(1, queues["other_queue"].listeners)
