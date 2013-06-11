from blueque import Client

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
