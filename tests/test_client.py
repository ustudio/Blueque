from blueque import Client

try:
    from unittest import mock
except ImportError:
    import mock

import unittest


class TestClient(unittest.TestCase):
    # The tests for the factory functions are in the tests for the
    # classes they create.

    @mock.patch("redis.StrictRedis", autospec=True)
    def test_client_connects_with_requested_information(self, mock_redis_class):
        self.client = Client("redis://url")

        mock_redis_class.from_url.assert_called_with("redis://url", decode_responses=True)

    @mock.patch("redis.StrictRedis", autospec=True)
    def test_client_passes_additional_args_to_client(self, mock_redis_class):
        self.client = Client("redis://url", 4, socket_timeout=5)

        mock_redis_class.from_url.assert_called_with(
            "redis://url", 4, decode_responses=True, socket_timeout=5)
