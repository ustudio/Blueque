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

    def _get_pipeline(self):
        return self.mock_redis.return_value.pipeline.return_value.__enter__.return_value

    def test_enqueue(self):
        pipeline = self._get_pipeline()

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

    def test_dequeue(self):
        mock_client = self.mock_redis.return_value

        mock_client.rpoplpush.return_value = "1234"

        task_id = self.queue.dequeue("some_node")

        self.assertEqual("1234", task_id)

        mock_client.rpoplpush.assert_called_with("some.queue", "some_node")
        mock_client.hmset.assert_called_with("1234", {"status": "reserved", "node": "some_node"})

    def test_start_task(self):
        pipeline = self._get_pipeline()

        self.queue.start("some_task", "some_node", 4321)

        pipeline.sadd.assert_called_with("running_tasks", "some_node 4321 some_task")
        pipeline.hmset.assert_called_with("some_task", {"status": "started", "pid": 4321})
