import os
import redis
import socket
from unittest import TestCase

import blueque


class TestIntegration(TestCase):
    def setUp(self):
        super().setUp()

        redis_client = redis.StrictRedis.from_url(os.environ["REDIS_URI"])
        redis_client.flushdb()

    def test_task_can_be_enqueued_and_return_result(self):
        producer_client = blueque.Client(os.environ["REDIS_URI"])
        producer_queue = producer_client.get_queue("QUEUE-NAME")

        worker_client = blueque.Client(os.environ["REDIS_URI"])
        worker_listener = worker_client.get_listener("QUEUE-NAME")

        task_id = producer_queue.enqueue("PARAMETERS")

        self.assertEqual("pending", producer_client.get_task(task_id).status)

        task = worker_listener.listen()

        self.assertEqual(task_id, task.id)
        self.assertEqual("PARAMETERS", task.parameters)

        self.assertEqual("reserved", producer_client.get_task(task_id).status)
        self.assertEqual(
            f"{socket.getfqdn()}_{os.getpid()}", producer_client.get_task(task_id).node)

        processor = worker_client.get_processor(task)

        processor.start(123456)

        self.assertEqual("started", producer_client.get_task(task_id).status)
        self.assertEqual(123456, producer_client.get_task(task_id).pid)

        processor.complete("RESULT")

        self.assertEqual("complete", producer_client.get_task(task_id).status)
        self.assertEqual("RESULT", producer_client.get_task(task_id).result)

        producer_queue.delete_task(producer_client.get_task(task_id))

        self.assertIsNone(producer_client.get_task(task_id).status)
