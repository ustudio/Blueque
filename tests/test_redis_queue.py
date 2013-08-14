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
        self.mock_time = self.time_patch.start()

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
        pipeline.zincrby.assert_called_with("blueque_queues", "some.queue", amount=1)

        pipeline.execute.assert_called_with()

        self.log_info.assert_called_with("Blueque queue some.queue: adding listener some_node")

    def test_remove_listener(self):
        self.mock_redis.srem.return_value = 1

        removed = self.queue.remove_listener("some_node")

        self.assertEqual(1, removed, "Returns one removed listener")

        self.mock_redis.zincrby.assert_called_with("blueque_queues", "some.queue", amount=-1)
        self.mock_redis.srem.assert_called_with("blueque_listeners_some.queue", "some_node")

        self.log_info.assert_has_calls([
            mock.call("Blueque queue some.queue: removing listener some_node"),
            mock.call("Blueque queue some.queue: removed listener")])

    def test_remove_missing_listener(self):
        self.mock_redis.srem.return_value = 0

        removed = self.queue.remove_listener("some_node")

        self.assertEqual(0, removed, "Returns no nodes removed")

        self.mock_redis.zincrby.assert_not_called()
        self.mock_redis.srem.assert_called_with("blueque_listeners_some.queue", "some_node")

        self.log_info.assert_called_with("Blueque queue some.queue: removing listener some_node")

    def test_get_listeners(self):
        self.mock_redis.smembers.return_value = ["some-listener_1234", "other-listener_4321"]

        listeners = self.queue.get_listeners()

        self.mock_redis.smembers.assert_called_with("blueque_listeners_some.queue")
        self.assertEqual(["some-listener_1234", "other-listener_4321"], listeners)

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

        pipeline.zincrby.assert_called_with("blueque_queues", "some.queue", amount=0)
        pipeline.lpush.assert_called_with(
            "blueque_pending_tasks_some.queue", "12345678-1234-1234-1234-123456781234")
        pipeline.execute.assert_called_with()

        self.log_info.assert_called_with(
            "Blueque queue some.queue: adding pending task 12345678-1234-1234-1234-123456781234, parameters: some parameter")

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

    def test_delete_completed_task(self):
        pipeline = self._get_pipeline()

        self.queue.delete_task("some_task", "complete")

        pipeline.delete.assert_called_with("blueque_task_some_task")
        pipeline.lrem.assert_called_with("blueque_complete_tasks_some.queue", 1, "some_task")

        pipeline.execute.assert_called_with()

        self.log_info.assert_called_with(
            "Blueque queue some.queue: deleting task some_task with status complete")

    def test_delete_failed_task(self):
        pipeline = self._get_pipeline()

        self.queue.delete_task("some_task", "failed")

        pipeline.delete.assert_called_with("blueque_task_some_task")
        pipeline.lrem.assert_called_with("blueque_failed_tasks_some.queue", 1, "some_task")

        pipeline.execute.assert_called_with()

        self.log_info.assert_called_with(
            "Blueque queue some.queue: deleting task some_task with status failed")

    def test_cannot_delete_unfinished_task(self):
        with self.assertRaisesRegexp(ValueError, "Cannot delete task with status started"):
            self.queue.delete_task("some_task", "started")

    def test_schedule_task(self):
        pipeline = self._get_pipeline()

        task_id = self.queue.schedule("some parameters", 13.5)

        self.assertEqual("12345678-1234-1234-1234-123456781234", task_id)

        pipeline.hmset.assert_called_with(
            "blueque_task_12345678-1234-1234-1234-123456781234",
            {
                "status": "scheduled",
                "queue": "some.queue",
                "parameters": "some parameters",
                "eta": 13.5,
                "created": 12.34,
                "updated": 12.34
            }
        )

        pipeline.zincrby.assert_called_with("blueque_queues", "some.queue", amount=0)

        pipeline.zadd.assert_called_with(
            "blueque_scheduled_tasks_some.queue", 13.5, "12345678-1234-1234-1234-123456781234")

        pipeline.execute.assert_called_with()

        self.log_info.assert_called_with(
            "Blueque queue some.queue: adding scheduled task 12345678-1234-1234-1234-123456781234, parameters: some parameters")

    def test_schedule_with_past_eta_just_enqueues(self):
        self.queue.enqueue = mock.Mock()
        self.queue.enqueue.return_value = "some_task"

        task_id = self.queue.schedule("some parameters", 1.0)

        self.assertEqual("some_task", task_id)

        self.queue.enqueue.assert_called_with("some parameters")

        self.assertFalse(self._get_pipeline().zadd.called)

    def test_enqueue_due_enqueues_all_due_tasks(self):
        pipeline = mock.MagicMock(spec=redis.client.StrictPipeline)

        pipeline.zrangebyscore.return_value = ["some_task", "other_task"]

        self.mock_redis.transaction.side_effect = lambda c, *args: c(pipeline)

        # make sure that we're snapshotting the current time, instead
        # of calling it multiple times.
        self.mock_time.side_effect = [10, 11, 12, 13]

        self.queue.enqueue_due_tasks()

        # don't care about the first argument, just that we're watching the right keys
        self.assertEqual(1, self.mock_redis.transaction.call_count)
        self.assertEqual(
            ("blueque_scheduled_tasks_some.queue",),
            self.mock_redis.transaction.call_args[0][1:])

        pipeline.zrangebyscore.assert_called_with("blueque_scheduled_tasks_some.queue", 0, 10)

        pipeline.multi.assert_called_with()
        pipeline.zremrangebyscore.assert_called_with("blueque_scheduled_tasks_some.queue", 0, 10)
        pipeline.lpush.assert_has_calls([
            mock.call("blueque_pending_tasks_some.queue", "some_task"),
            mock.call("blueque_pending_tasks_some.queue", "other_task")
        ])

        pipeline.hmset.assert_has_calls([
            mock.call("blueque_task_some_task", {"status": "pending", "updated": 10}),
            mock.call("blueque_task_other_task", {"status": "pending", "updated": 10})
        ])

        self.log_info.assert_called_with(
            "Blueque queue some.queue: enqueuing due tasks: ['some_task', 'other_task']")

    def test_enqueue_due_does_nothing_when_nothing_is_due(self):
        pipeline = mock.MagicMock(spec=redis.client.StrictPipeline)

        pipeline.zrangebyscore.return_value = []

        self.mock_redis.transaction.side_effect = lambda c, *args: c(pipeline)

        # make sure that we're snapshotting the current time, instead
        # of calling it multiple times.
        self.mock_time.side_effect = [10, 11, 12, 13]

        self.queue.enqueue_due_tasks()

        # don't care about the first argument, just that we're watching the right keys
        self.assertEqual(1, self.mock_redis.transaction.call_count)
        self.assertEqual(
            ("blueque_scheduled_tasks_some.queue",),
            self.mock_redis.transaction.call_args[0][1:])

        self.assertFalse(pipeline.zremrangebyscore.called)

        self.log_info.assert_called_with("Blueque queue some.queue: no due tasks")
