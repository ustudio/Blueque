from blueque import Client

import mock
import unittest


class TestProcessor(unittest.TestCase):
    @mock.patch("redis.StrictRedis", autospec=True)
    @mock.patch("blueque.client.RedisQueue", autospec=True)
    def setUp(self, mock_redis_queue_class, mock_strict_redis):
        self.mock_redis_queue = mock_redis_queue_class.return_value
        self.mock_strict_redis = mock_strict_redis.return_value

        self.mock_strict_redis.hgetall.return_value = {
            "queue": "some.queue",
            "status": "reserved",
            "node": "host_1234",
            "parameters": "some parameters"
        }

        self.client = Client(hostname="asdf", port=1234, db=0)
        self.task = self.client.get_task("some_task")
        self.processor = self.client.get_processor(self.task)

    def test_start_starts_processor(self):
        self.processor.start(4321)

        self.mock_redis_queue.start.assert_called_with("some_task", "host_1234", 4321)

    def test_complete_marks_task_completed(self):
        # Can't complete an unstarted process
        self.processor.start(4321)

        self.processor.complete("some result")

        self.mock_redis_queue.complete.assert_called_with(
            "some_task", "host_1234", 4321, "some result")

    def test_fail_marks_task_failed(self):
        # Can't complete an unstarted process
        self.processor.start(4321)

        self.processor.fail("some error")

        self.mock_redis_queue.fail.assert_called_with(
            "some_task", "host_1234", 4321, "some error")
