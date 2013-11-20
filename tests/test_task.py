from blueque import Client

import mock
import unittest


FULL_TASK_DATA = {
    "status": "complete",
    "queue": "some.queue",
    "parameters": "some parameters",
    "result": "a result",
    "error": "no error",
    "node": "some_node",
    "pid": "1234",
    "created": "1234.5",
    "updated": "4567.89"
}


class TestTask(unittest.TestCase):
    @mock.patch("redis.StrictRedis", autospec=True)
    def setUp(self, redis_class):
        self.mock_redis = redis_class.return_value

        self.client = Client("asdf", 1234, 0)

    def test_can_get_task_with_all_attributes(self):
        self.mock_redis.hgetall.return_value = FULL_TASK_DATA

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

    def test_cannot_set_properties(self):
        self.mock_redis.hgetall.return_value = FULL_TASK_DATA

        task = self.client.get_task("some_task")

        with self.assertRaises(AttributeError):
            task.id = "foo"

        with self.assertRaises(AttributeError):
            task.status = "foo"

        with self.assertRaises(AttributeError):
            task.queue = "foo"

        with self.assertRaises(AttributeError):
            task.parameters = "foo"

        with self.assertRaises(AttributeError):
            task.result = "foo"

        with self.assertRaises(AttributeError):
            task.error = "foo"

        with self.assertRaises(AttributeError):
            task.node = "foo"

        with self.assertRaises(AttributeError):
            task.pid = 4321

        with self.assertRaises(AttributeError):
            task.created = 1.2

        with self.assertRaises(AttributeError):
            task.updated = 2.3

    def test_missing_attributes_are_none(self):
        self.mock_redis.hgetall.return_value = {}

        task = self.client.get_task("some_task")

        self.assertEqual(None, task.status)
        self.assertEqual(None, task.queue)
        self.assertEqual(None, task.parameters)
        self.assertEqual(None, task.result)
        self.assertEqual(None, task.error)
        self.assertEqual(None, task.node)
        self.assertEqual(None, task.pid)
        self.assertEqual(None, task.created)
        self.assertEqual(None, task.updated)
