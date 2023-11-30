import os
import redis
import socket
from unittest import mock, skipUnless, TestCase

import blueque


@skipUnless("REDIS_URI" in os.environ, "REDIS_URI required to run integration tests.")
class TestIntegration(TestCase):
    def setUp(self):
        super().setUp()

        redis_client = redis.StrictRedis.from_url(os.environ["REDIS_URI"])
        redis_client.flushdb()

        self.producer_client = blueque.Client(os.environ["REDIS_URI"])
        self.producer_queue = self.producer_client.get_queue("QUEUE-NAME")

        self.worker_client = blueque.Client(os.environ["REDIS_URI"])
        self.worker_listener = self.worker_client.get_listener("QUEUE-NAME")

    def test_task_can_be_enqueued_and_return_result(self):
        task_id = self.producer_queue.enqueue("PARAMETERS")

        self.assertEqual("pending", self.producer_client.get_task(task_id).status)

        task = self.worker_listener.listen()

        self.assertEqual(task_id, task.id)
        self.assertEqual("PARAMETERS", task.parameters)

        self.assertEqual("reserved", self.producer_client.get_task(task_id).status)
        self.assertEqual(
            f"{socket.getfqdn()}_{os.getpid()}", self.producer_client.get_task(task_id).node)

        processor = self.worker_client.get_processor(task)

        processor.start(123456)

        self.assertEqual("started", self.producer_client.get_task(task_id).status)
        self.assertEqual(123456, self.producer_client.get_task(task_id).pid)

        processor.complete("RESULT")

        self.assertEqual("complete", self.producer_client.get_task(task_id).status)
        self.assertEqual("RESULT", self.producer_client.get_task(task_id).result)
        self.assertIsNone(self.producer_client.get_task(task_id).error)

        self.producer_queue.delete_task(self.producer_client.get_task(task_id))

        self.assertIsNone(self.producer_client.get_task(task_id).status)

    def test_task_can_return_failing_status(self):
        task_id = self.producer_queue.enqueue("PARAMETERS")

        task = self.worker_listener.listen()

        processor = self.worker_client.get_processor(task)

        processor.start(123456)

        processor.fail("ERROR")

        self.assertEqual("failed", self.producer_client.get_task(task_id).status)
        self.assertEqual("ERROR", self.producer_client.get_task(task_id).error)
        self.assertIsNone(self.producer_client.get_task(task_id).result)

        self.producer_queue.delete_task(self.producer_client.get_task(task_id))

    def test_orphaned_task_can_be_claimed(self):
        task_id = self.producer_queue.enqueue("PARAMETERS")

        pid = os.fork()
        if pid == 0:
            listener = self.worker_client.get_listener("QUEUE-NAME")
            task = listener.listen()
            processor = self.worker_client.get_processor(task)
            processor.start(os.getpid())
            os._exit(0)

        os.waitpid(pid, 0)

        orphaned_task = self.worker_listener.claim_orphan()

        self.assertEqual(task_id, orphaned_task.id)
        self.assertEqual(f"{socket.getfqdn()}_{pid}", self.producer_client.get_task(task_id).node)
        self.assertEqual("started", orphaned_task.status)

        self.assertCountEqual(
            [f"{socket.getfqdn()}_{os.getpid()}"], self.producer_queue._redis_queue.get_listeners())

    @mock.patch("time.time")
    def test_scheduled_tasks_are_enqueued(self, mock_time):
        mock_time.return_value = 1000

        task_id = self.producer_queue.schedule("PARAMETERS", 1005)

        self.assertEqual("scheduled", self.producer_client.get_task(task_id).status)

        mock_time.return_value = 1004

        self.producer_queue.enqueue_due_tasks()

        self.assertEqual("scheduled", self.producer_client.get_task(task_id).status)

        mock_time.return_value = 1005

        self.producer_queue.enqueue_due_tasks()

        self.assertEqual("pending", self.producer_client.get_task(task_id).status)

        task = self.worker_listener.listen()

        self.assertEqual(task_id, task.id)
        self.assertEqual("PARAMETERS", task.parameters)
