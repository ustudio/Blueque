from blueque import Client

import mock
import unittest


class BreakLoopException(RuntimeError):
    pass


class TestListener(unittest.TestCase):
    @mock.patch("socket.getfqdn", return_value="somehost.example.com")
    @mock.patch("os.getpid", return_value=2314)
    @mock.patch("redis.StrictRedis", autospec=True)
    @mock.patch("blueque.client.RedisQueue", autospec=True)
    def setUp(self, mock_redis_queue_class, mock_strict_redis, _, __):
        self.mock_strict_redis = mock_strict_redis

        self.mock_redis_queue_class = mock_redis_queue_class
        self.mock_redis_queue = mock_redis_queue_class.return_value

        self.client = Client("asdf", 1234, 0)
        self.listener = self.client.get_listener("some.queue")

    def test_listener_adds_itself(self):
        self.mock_redis_queue.add_listener.assert_called_with("somehost.example.com_2314")

    def test_listener_calls_callback_when_task_in_queue(self):
        callback = mock.MagicMock()

        self.mock_redis_queue.dequeue.side_effect = ["some_task", BreakLoopException()]

        try:
            self.listener.listen(callback)
        except BreakLoopException:
            pass

        self.mock_redis_queue.dequeue.assert_called_with("somehost.example.com_2314")
        callback.assert_called_with("some_task")

    def test_listener_calls_callback_multiple_times(self):
        callback = mock.MagicMock()

        self.mock_redis_queue.dequeue.side_effect = [
            "some_task", "other_task", BreakLoopException()]

        try:
            self.listener.listen(callback)
        except BreakLoopException:
            pass

        self.mock_redis_queue.dequeue.assert_has_calls(
            [mock.call("somehost.example.com_2314"), mock.call("somehost.example.com_2314")])

        callback.assert_has_calls([mock.call("some_task"), mock.call("other_task")])

    @mock.patch("time.sleep", autospec=True)
    def test_listener_sleeps_when_no_task_available(self, mock_sleep):
        callback = mock.MagicMock()

        self.mock_redis_queue.dequeue.side_effect = [None, "some_task", BreakLoopException()]

        try:
            self.listener.listen(callback)
        except BreakLoopException:
            pass

        self.mock_redis_queue.dequeue.assert_has_calls(
            [mock.call("somehost.example.com_2314"), mock.call("somehost.example.com_2314")])

        callback.assert_has_calls([mock.call("some_task")])

        mock_sleep.assert_has_calls([mock.call(1)])
