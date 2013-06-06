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
        pipeline.execute.assert_called_with()

    def test_dequeue(self):
        mock_client = self.mock_redis.return_value

        mock_client.rpoplpush.return_value = "1234"

        task_id = self.queue.dequeue("some_node")

        self.assertEqual("1234", task_id)

        mock_client.rpoplpush.assert_called_with("some.queue", "some_node")
        mock_client.hmset.assert_called_with("1234", {"status": "reserved", "node": "some_node"})

    def test_start_task(self):
        pipeline = self._get_pipeline()

        pipeline.execute.return_value = [1, True, json.dumps({"some": "parameter"})]

        parameters = self.queue.start("some_task", "some_node", 4321)

        self.assertEqual({"some": "parameter"}, parameters)

        pipeline.sadd.assert_called_with("running_tasks", "some_node 4321 some_task")
        pipeline.hmset.assert_called_with("some_task", {"status": "started", "pid": 4321})
        pipeline.hget.assert_called_with("some_task", "parameters")

        pipeline.execute.assert_called_with()

    def test_complete_task(self):
        pipeline = self._get_pipeline()

        self.queue.complete("some_task", "some_node", 1234, {"a": "result"})

        pipeline.lrem.assert_has_calls([
            mock.call("some_node", "some_task"),
            mock.call("running_tasks", 1, "some_node 1234 some_task")])

        pipeline.hmset.assert_called_with(
            "some_task", {"status": "complete", "result": json.dumps({"a": "result"})})

        pipeline.lpush.assert_called_with("complete", "some_task")

        pipeline.execute.assert_called_with()
