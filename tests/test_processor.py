from blueque.processor import Processor
from blueque.redis_queue import RedisQueue

import mock
import unittest


class TestProcessor(unittest.TestCase):
    def setUp(self):
        self.mock_redis_queue = mock.MagicMock(spec=RedisQueue)

        self.processor = Processor("host_1234", "some_task", self.mock_redis_queue)

    def test_start_starts_processor(self):
        self.mock_redis_queue.start.return_value = "some parameters"

        parameters = self.processor.start(4321)

        self.assertEqual("some parameters", parameters)
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
