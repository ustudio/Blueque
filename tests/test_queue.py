from blueque.queue import Queue

import json
import mock
import unittest


class TestQueue(unittest.TestCase):
    def setUp(self):
        self.redis_patch = mock.patch("redis.StrictRedis", autospec=True)
        self.mock_redis = self.redis_patch.start()

        self.uuid_patch = mock.patch("uuid.uuid4", return_value="1234567890")
        self.uuid_patch.start()

        self.queue = Queue("some.queue")

    def tearDown(self):
        self.redis_patch.stop()
        self.uuid_patch.stop()

    def test_enqueue(self):
        pipeline = self.mock_redis.return_value.pipeline.return_value.__enter__.return_value

        task_id = self.queue.enqueue({"some": "parameter"})

        self.assertEqual("1234567890", task_id)

        pipeline.hmset.assert_called_with(
            "1234567890",
            {
                "status": "pending",
                "queue": "some.queue",
                "parameters": json.dumps({"some": "parameter"})
            })

        pipeline.lpush.assert_called_with("some.queue", "1234567890")
