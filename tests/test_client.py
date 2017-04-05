from blueque import Client

import mock
import unittest


class TestClient(unittest.TestCase):
    # The tests for the factory functions are in the tests for the
    # classes they create.

    @mock.patch("redis.StrictRedis", autospec=True)
    def test_client_connects_with_requested_information(self, mock_redis):
        self.client = Client(hostname="foo", port=1234, db=0)

        mock_redis.assert_called_with(host="foo", port=1234, db=0)

    @mock.patch("redis.StrictRedis", autospec=True)
    def test_client_passes_additional_args_to_client(self, mock_redis):
        self.client = Client(hostname="foo", port=1234, db=0, password="password", charset="utf8")

        mock_redis.assert_called_with(
            host="foo", port=1234, db=0, password="password", charset="utf8")
