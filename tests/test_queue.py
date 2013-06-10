from blueque import Client

import mock
import unittest


class TestQueue(unittest.TestCase):
    @mock.patch("redis.StrictRedis", autospec=True)
    @mock.patch("blueque.client.RedisQueue", autospec=True)
    def setUp(self, mock_redis_queue_class, mock_strict_redis):
        self.mock_strict_redis = mock_strict_redis

        self.mock_redis_queue_class = mock_redis_queue_class
        self.mock_redis_queue = mock_redis_queue_class.return_value

        self.client = Client("asdf", 1234, 0)
        self.queue = self.client.get_queue("some.queue")

    def test_name_passed_to_redis_queue(self):
        self.mock_redis_queue_class.assert_called_with(
            "some.queue", self.mock_strict_redis.return_value)

    def test_enqueue_enqueues_task(self):
        self.mock_redis_queue.enqueue.return_value = "task_id"

        task_id = self.queue.enqueue("the parameters")

        self.assertEqual("task_id", task_id)
