from blueque import Client

try:
    from unittest import mock
except ImportError:
    import mock

import unittest


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
        self.mock_strict_redis.return_value.hgetall.return_value = {
            "parameters": "some parameters"
        }
        self.mock_redis_queue.dequeue.side_effect = ["some_task"]

        task = self.listener.listen()

        self.mock_redis_queue.dequeue.assert_called_with("somehost.example.com_2314")

        self.assertEqual("some_task", task.id)
        self.assertEqual("some parameters", task.parameters)

    @mock.patch("time.sleep", autospec=True)
    def test_listener_sleeps_when_no_task_available(self, mock_sleep):
        self.mock_redis_queue.dequeue.side_effect = [None, "some_task"]

        task = self.listener.listen()

        self.mock_redis_queue.dequeue.assert_has_calls(
            [mock.call("somehost.example.com_2314"), mock.call("somehost.example.com_2314")])

        mock_sleep.assert_has_calls([mock.call(1)])

        self.assertEqual("some_task", task.id)

    def test_claim_orphan_returns_none_when_there_are_no_listeners(self):
        self.mock_redis_queue.get_listeners.return_value = []

        claimed = self.listener.claim_orphan()

        self.mock_redis_queue.get_listeners.assert_called_with()
        self.assertIsNone(claimed)

    @mock.patch("os.kill")
    def test_claim_orphan_returns_none_when_there_are_no_listeners_on_this_node(self, mock_kill):
        self.mock_redis_queue.get_listeners.return_value = ["other-host_4321"]

        claimed = self.listener.claim_orphan()

        self.mock_redis_queue.get_listeners.assert_called_with()
        self.assertIsNone(claimed)

        mock_kill.assert_not_called()

    @mock.patch("os.kill")
    def test_claim_orphan_returns_none_when_listener_is_self(self, mock_kill):
        self.mock_redis_queue.get_listeners.return_value = ["somehost.example.com_2314"]

        claimed = self.listener.claim_orphan()

        self.mock_redis_queue.get_listeners.assert_called_with()
        self.assertIsNone(claimed)

        mock_kill.assert_not_called()

    @mock.patch("os.kill", return_value=None)
    def test_claim_orphan_returns_none_when_listener_is_running(self, mock_kill):
        self.mock_redis_queue.get_listeners.return_value = ["somehost.example.com_4321"]

        claimed = self.listener.claim_orphan()

        self.mock_redis_queue.get_listeners.assert_called_with()
        self.assertIsNone(claimed)

        mock_kill.assert_called_with(4321, 0)

    @mock.patch("os.kill", side_effect=OSError)
    def test_claim_orphan_returns_none_when_orphan_claimed(self, mock_kill):
        self.mock_redis_queue.get_listeners.return_value = ["somehost.example.com_4321"]
        self.mock_redis_queue.remove_listener.return_value = 0

        claimed = self.listener.claim_orphan()

        self.mock_redis_queue.get_listeners.assert_called_with()
        self.mock_redis_queue.remove_listener.assert_called_with("somehost.example.com_4321")
        mock_kill.assert_called_with(4321, 0)

        self.assertIsNone(claimed)

    @mock.patch("os.kill", side_effect=OSError)
    def test_claim_orphan_returns_none_when_no_tasks_reserved(self, mock_kill):
        self.mock_redis_queue.get_listeners.return_value = ["somehost.example.com_4321"]
        self.mock_redis_queue.remove_listener.return_value = 1
        self.mock_redis_queue.reclaim_task.return_value = None

        claimed = self.listener.claim_orphan()

        self.mock_redis_queue.get_listeners.assert_called_with()
        self.mock_redis_queue.remove_listener.assert_called_with("somehost.example.com_4321")
        mock_kill.assert_called_with(4321, 0)
        self.mock_redis_queue.reclaim_task.assert_called_with(
            "somehost.example.com_4321", "somehost.example.com_2314")

        self.assertIsNone(claimed)

    @mock.patch("os.kill", side_effect=OSError)
    def test_claim_orphan_returns_task_when_reclaimed(self, mock_kill):
        self.mock_redis_queue.get_listeners.return_value = ["somehost.example.com_4321"]
        self.mock_redis_queue.remove_listener.return_value = 1
        self.mock_redis_queue.reclaim_task.return_value = "some_task"

        self.mock_strict_redis.return_value.hgetall.return_value = {
            "parameters": "some parameters"
        }

        claimed = self.listener.claim_orphan()

        self.mock_redis_queue.get_listeners.assert_called_with()
        self.mock_redis_queue.remove_listener.assert_called_with("somehost.example.com_4321")
        mock_kill.assert_called_with(4321, 0)
        self.mock_redis_queue.reclaim_task.assert_called_with(
            "somehost.example.com_4321", "somehost.example.com_2314")
        self.mock_strict_redis.return_value.hgetall.assert_called_with("blueque_task_some_task")

        self.assertIsNotNone(claimed)
        self.assertEqual("some parameters", claimed.parameters)
