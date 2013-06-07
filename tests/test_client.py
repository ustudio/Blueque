from blueque import Client

import mock
import unittest


class TestClient(unittest.TestCase):

    @mock.patch("redis.StrictRedis", autospec=True)
    def setUp(self, mock_redis):
        self.client = Client(hostname="foo", port=1234, db=0)
        self.mock_redis = mock_redis

    def test_client_connects_with_requested_information(self):
        self.mock_redis.assert_called_with(host="foo", port=1234, db=0)
