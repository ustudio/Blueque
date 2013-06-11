from blueque import Client

import mock
import unittest


class TestTask(unittest.TestCase):
    @mock.patch("redis.StrictRedis", autospec=True)
    def setUp(self, redis_class):
        self.mock_redis = redis_class.return_value

        self.client = Client("asdf", 1234, 0)

    def test_can_get_task_with_all_attributes(self):
        self.mock_redis.hgetall.return_value = {
            "status": "complete",
            "queue": "some.queue",
            "parameters": "some parameters",
            "result": "a result",
            "error": "no error",
            "node": "some_node",
            "pid": 1234,
            "created": 1234.5,
            "updated": 4567.89
        }

        task = self.client.get_task("some_task")

        self.mock_redis.hgetall.assert_called_with("blueque_task_some_task")

        self.assertEqual("some_task", task.id)

        self.assertEqual("complete", task.status)
        self.assertEqual("some.queue", task.queue)
        self.assertEqual("some parameters", task.parameters)
        self.assertEqual("a result", task.result)
        self.assertEqual("no error", task.error)
        self.assertEqual("some_node", task.node)
        self.assertEqual(1234, task.pid)
        self.assertEqual(1234.5, task.created)
        self.assertEqual(4567.89, task.updated)
