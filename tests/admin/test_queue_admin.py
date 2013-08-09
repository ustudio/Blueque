import mock
import redis
import unittest

from blueque.admin.queue_admin import QueueAdmin


class TestAdmin(unittest.TestCase):
    def setUp(self):
        self.mock_redis = mock.MagicMock(spec=redis.StrictRedis)
        self.queue_admin = QueueAdmin(self.mock_redis, "some_queue", 2)

    def test_has_name_and_listeners(self):
        self.assertEqual("some_queue", self.queue_admin.name)
        self.assertEqual(2, self.queue_admin.listeners)

    def test_returns_empty_list_with_no_tasks(self):
        self.mock_redis.smembers.return_value = set([])

        tasks = self.queue_admin.get_tasks()

        self.mock_redis.smembers.assert_called_with("blueque_all_tasks_some_queue")

        self.assertEqual(0, len(tasks))

    def test_returns_two_tasks_when_present(self):
        self.mock_redis.smembers.return_value = set(["some_queue", "other_queue"])

        tasks = self.queue_admin.get_tasks()

        self.mock_redis.smembers.assert_called_with("blueque_all_tasks_some_queue")

        self.assertEqual(2, len(tasks))
