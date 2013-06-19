from blueque.redis_queue import RedisQueue

import mock
import redis
import unittest
import uuid


class TestRedisQueue(unittest.TestCase):
    def setUp(self):
        self.mock_redis = mock.MagicMock(spec=redis.StrictRedis)

        self.uuid_patch = mock.patch(
            "uuid.uuid4", return_value=uuid.UUID("{12345678-1234-1234-1234-123456781234}"))
        self.uuid_patch.start()

        self.time_patch = mock.patch("time.time", return_value=12.34)
        self.time_patch.start()

        self.log_info_patch = mock.patch("logging.info", autospec=True)
        self.log_info = self.log_info_patch.start()

        self.queue = RedisQueue("some.queue", self.mock_redis)

    def tearDown(self):
        self.uuid_patch.stop()
        self.time_patch.stop()
        self.log_info_patch.stop()

    def _get_pipeline(self):
        return self.mock_redis.pipeline.return_value.__enter__.return_value

    def test_add_listener(self):
        pipeline = self._get_pipeline()

        self.queue.add_listener("some_node")

        pipeline.sadd.assert_called_with("blueque_listeners_some.queue", "some_node")
        pipeline.zincrby.assert_called_with("blueque_queues", 1, "some.queue")

        pipeline.execute.assert_called_with()

        self.log_info.assert_called_with("Blueque queue some.queue: adding listener some_node")

    def test_remove_listener(self):
        pipeline = self._get_pipeline()

        self.queue.remove_listener("some_node")

        pipeline.zincrby.assert_called_with("blueque_queues", -1, "some.queue")
        pipeline.srem.assert_called_with("blueque_listeners_some.queue", "some_node")

        pipeline.execute.assert_called_with()

        self.log_info.assert_called_with("Blueque queue some.queue: removing listener some_node")

    def test_enqueue(self):
        pipeline = self._get_pipeline()

        task_id = self.queue.enqueue("some parameter")

        self.assertEqual("12345678-1234-1234-1234-123456781234", task_id)

        pipeline.hmset.assert_called_with(
            "blueque_task_12345678-1234-1234-1234-123456781234",
            {
                "status": "pending",
                "queue": "some.queue",
                "parameters": "some parameter",
                "created": 12.34,
                "updated": 12.34
            })

        pipeline.zincrby.assert_called_with("blueque_queues", 0, "some.queue")
        pipeline.lpush.assert_called_with(
            "blueque_pending_tasks_some.queue", "12345678-1234-1234-1234-123456781234")
        pipeline.execute.assert_called_with()

        self.log_info.assert_called_with(
            "Blueque queue some.queue: adding task 12345678-1234-1234-1234-123456781234, parameters: some parameter")

    def test_dequeue(self):
        self.mock_redis.rpoplpush.return_value = "1234"

        task_id = self.queue.dequeue("some_node")

        self.assertEqual("1234", task_id)

        self.mock_redis.rpoplpush.assert_called_with(
            "blueque_pending_tasks_some.queue", "blueque_reserved_tasks_some.queue_some_node")
        self.mock_redis.hmset.assert_called_with(
            "blueque_task_1234", {"status": "reserved", "node": "some_node", "updated": 12.34})

        self.log_info.assert_has_calls([
            mock.call("Blueque queue some.queue: reserving task on some_node"),
            mock.call("Blueque queue some.queue: got task 1234")
        ])

    def test_dequeue_returns_null_when_empty(self):
        self.mock_redis.rpoplpush.return_value = None

        task_id = self.queue.dequeue("some_node")

        self.assertEqual(None, task_id)

        self.mock_redis.rpoplpush.assert_called_with(
            "blueque_pending_tasks_some.queue", "blueque_reserved_tasks_some.queue_some_node")
        self.mock_redis.hmset.assert_has_calls([])

        self.log_info.assert_has_calls([
            mock.call("Blueque queue some.queue: reserving task on some_node")
        ])

    def test_start_task(self):
        pipeline = self._get_pipeline()

        pipeline.execute.return_value = [1, True, "some parameter"]

        self.queue.start("some_task", "some_node", 4321)

        pipeline.sadd.assert_called_with(
            "blueque_started_tasks_some.queue", "some_node 4321 some_task")

        pipeline.hmset.assert_called_with(
            "blueque_task_some_task", {"status": "started", "pid": 4321, "updated": 12.34})

        pipeline.execute.assert_called_with()

        self.log_info.assert_has_calls([
            mock.call("Blueque queue some.queue: starting task some_task on some_node, pid 4321")
        ])

    def test_complete_task(self):
        pipeline = self._get_pipeline()

        self.queue.complete("some_task", "some_node", 1234, "a result")

        pipeline.lrem.assert_called_with("blueque_reserved_tasks_some.queue_some_node", 1, "some_task")
        pipeline.srem.assert_called_with(
            "blueque_started_tasks_some.queue", "some_node 1234 some_task")

        pipeline.hmset.assert_called_with(
            "blueque_task_some_task",
            {
                "status": "complete",
                "result": "a result",
                "updated": 12.34
            })

        pipeline.lpush.assert_called_with("blueque_complete_tasks_some.queue", "some_task")

        pipeline.execute.assert_called_with()

        self.log_info.assert_called_with(
            "Blueque queue some.queue: completing task some_task on some_node, pid: 1234, result: a result")

    def test_fail_task(self):
        pipeline = self._get_pipeline()

        self.queue.fail("some_task", "some_node", 1234, "error message")

        pipeline.lrem.assert_called_with("blueque_reserved_tasks_some.queue_some_node", 1, "some_task")
        pipeline.srem.assert_called_with(
            "blueque_started_tasks_some.queue", "some_node 1234 some_task")

        pipeline.hmset.assert_called_with(
            "blueque_task_some_task",
            {
                "status": "failed",
                "error": "error message",
                "updated": 12.34
            })

        pipeline.lpush.assert_called_with("blueque_failed_tasks_some.queue", "some_task")

        pipeline.execute.assert_called_with()

        self.log_info.assert_called_with(
            "Blueque queue some.queue: failed task some_task on some_node, pid: 1234, error: error message")
