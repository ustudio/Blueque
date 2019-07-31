from blueque import Client

try:
    from unittest import mock
except ImportError:
    import mock

import unittest


class TestQueue(unittest.TestCase):
    @mock.patch("redis.StrictRedis", autospec=True)
    @mock.patch("blueque.client.RedisQueue", autospec=True)
    def setUp(self, mock_redis_queue_class, mock_strict_redis):
        self.mock_strict_redis = mock_strict_redis.from_url.return_value

        self.mock_redis_queue_class = mock_redis_queue_class
        self.mock_redis_queue = mock_redis_queue_class.return_value

        self.client = Client("redis://asdf:1234")
        self.queue = self.client.get_queue("some.queue")

    def test_name_passed_to_redis_queue(self):
        self.mock_redis_queue_class.assert_called_with("some.queue", self.mock_strict_redis)

    def test_enqueue_enqueues_task(self):
        self.mock_redis_queue.enqueue.return_value = "task_id"

        task_id = self.queue.enqueue("the parameters")

        self.assertEqual("task_id", task_id)
        self.mock_redis_queue.enqueue.assert_called_with("the parameters")

    def test_schedule_schedules_task(self):
        self.mock_redis_queue.schedule.return_value = "task_id"

        task_id = self.queue.schedule("some parameters", 24.3)

        self.assertEqual("task_id", task_id)
        self.mock_redis_queue.schedule.assert_called_with("some parameters", 24.3)

    def test_enqueue_due_tasks_enqueues_due_tasks(self):
        self.queue.enqueue_due_tasks()

        self.mock_redis_queue.enqueue_due_tasks.assert_called_with()

    def test_delete_deletes_task(self):
        self.mock_strict_redis.hgetall.return_value = {
            "status": "complete",
            "queue": "some.queue"
        }

        task = self.client.get_task("some_task")

        self.queue.delete_task(task)

        self.mock_redis_queue.delete_task.assert_called_with("some_task", "complete")

    def test_delete_errors_on_wrong_queue(self):
        self.mock_strict_redis.hgetall.return_value = {
            "status": "complete",
            "queue": "other.queue"
        }

        task = self.client.get_task("some_task")

        with self.assertRaisesRegexp(ValueError, "Task some_task is not in queue some.queue"):
            self.queue.delete_task(task)
