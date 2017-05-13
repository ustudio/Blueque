import mock
import redis
import unittest

from blueque.admin.task_admin import TaskAdmin


class TestAdmin(unittest.TestCase):
    def setUp(self):
        self.mock_redis = mock.MagicMock(spec=redis.StrictRedis)

        self.mock_redis.hgetall.return_value = {
            "parameters": "some parameters"
        }

        self.task_admin = TaskAdmin(self.mock_redis, "some_task")

    def test_task_admin_has_task_properties(self):
        self.assertEqual("some_task", self.task_admin.id)
        self.assertEqual("some parameters", self.task_admin.parameters)
