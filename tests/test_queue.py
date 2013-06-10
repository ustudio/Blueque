from blueque.queue import Queue
from blueque.redis_queue import RedisQueue

import mock
import unittest


class TestQueue(unittest.TestCase):
    def setUp(self):
        self.redis_queue = mock.MagicMock(spec=RedisQueue)
        self.queue = Queue(self.redis_queue)

    def test_enqueue_enqueues_task(self):
        self.redis_queue.enqueue.return_value = "task_id"

        task_id = self.queue.enqueue("the parameters")

        self.assertEqual("task_id", task_id)
        self.redis_queue.enqueue.assert_called_with("the parameters")
