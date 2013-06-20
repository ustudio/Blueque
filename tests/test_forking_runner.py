from blueque import Client
from blueque import forking_runner

import mock
import unittest


class BreakLoop(RuntimeError):
    pass


@mock.patch("blueque.client.RedisQueue", autospec=True)
class TestForkingRunner(unittest.TestCase):
    @mock.patch("redis.StrictRedis", autospec=True)
    def setUp(self, strict_redis_class):
        self.mock_strict_redis = strict_redis_class.return_value

        self.task_callback = mock.Mock()

        self.client = Client(hostname="asdf", port=1234, db=0)
        self.runner = forking_runner.ForkingRunner(self.client, "some.queue", self.task_callback)

    def _get_task(self):
        self.mock_strict_redis.hgetall.return_value = {
            "status": "reserved",
            "parameters": "some params",
            "node": "some.host_1111"
        }

        return self.client.get_task("some_task")

    @mock.patch("os.fork", return_value=1234)
    @mock.patch("os.waitpid", return_value=(1234, 0))
    def test_run_listens_for_and_forks_task(self, mock_waitpid, mock_fork, redis_queue_class):
        mock_queue = redis_queue_class.return_value

        mock_queue.dequeue.side_effect = ["some_task", BreakLoop()]

        try:
            # Don't want a real thread.
            self.runner.run()
        except BreakLoop:
            pass

        self.assertTrue(self.runner.running.is_set())

        redis_queue_class.assert_called_with("some.queue", self.mock_strict_redis)

        mock_fork.assert_has_calls([mock.call()])
        mock_waitpid.assert_has_calls([mock.call(1234, 0)])

    @mock.patch("os.fork", side_effect=[1234, 4321])
    @mock.patch("os.waitpid", side_effect=[(1234, 0), (4321, 0)])
    def test_run_keeps_running_tasks(self, mock_waitpid, mock_fork, redis_queue_class):
        mock_queue = redis_queue_class.return_value

        mock_queue.dequeue.side_effect = ["some_task", "other_task", BreakLoop()]

        try:
            # Don't want a real thread.
            self.runner.run()
        except BreakLoop:
            pass

        redis_queue_class.assert_called_with("some.queue", self.mock_strict_redis)

        mock_fork.assert_has_calls([mock.call(), mock.call()])
        mock_waitpid.assert_has_calls([mock.call(1234, 0), mock.call(4321, 0)])

    @mock.patch("os.fork", return_value=1234)
    def test_fork_task_returns_pid_in_parent(self, mock_fork, redis_queue_class):
        task = self._get_task()

        pid = self.runner.fork_task(task)

        self.assertEqual(1234, pid)

    @mock.patch("random.seed")
    @mock.patch("os.getpid", return_value=2222)
    @mock.patch("os.setsid")
    @mock.patch("os.fork", return_value=0)
    @mock.patch("os._exit")
    def test_fork_task_runs_task_in_child(
            self, mock_exit, mock_fork, mock_setsid, _, mock_seed, redis_queue_class):
        mock_queue = redis_queue_class.return_value
        self.task_callback.return_value = "some result"

        task = self._get_task()

        self.runner.fork_task(task)

        mock_seed.assert_called_with()

        mock_setsid.assert_called_with()

        mock_queue.start.assert_called_with("some_task", "some.host_1111", 2222)

        self.task_callback.assert_called_with(task)

        mock_queue.complete.assert_called_with("some_task", "some.host_1111", 2222, "some result")

        mock_exit.assert_called_with(0)

    @mock.patch("os.getpid", return_value=2222)
    @mock.patch("os.setsid")
    @mock.patch("os.fork", return_value=0)
    @mock.patch("os._exit")
    def test_fork_task_fails_task_on_exception(
            self, mock_exit, mock_fork, mock_setsid, _, redis_queue_class):
        mock_queue = redis_queue_class.return_value

        callback_exception = Exception("some error")
        self.task_callback.side_effect = callback_exception

        task = self._get_task()

        self.runner.fork_task(task)

        mock_setsid.assert_called_with()

        mock_queue.start.assert_called_with("some_task", "some.host_1111", 2222)

        self.task_callback.assert_called_with(task)

        mock_queue.fail.assert_called_with(
            "some_task", "some.host_1111", 2222, str(callback_exception))

        mock_exit.assert_called_with(0)


@mock.patch("time.sleep", autospec=True)
@mock.patch("threading.Event", autospec=True)
@mock.patch.object(forking_runner.ForkingRunner, "start", autospec=True)
@mock.patch.object(forking_runner.ForkingRunner, "is_alive")
class TestRun(unittest.TestCase):
    @mock.patch("redis.StrictRedis", autospec=True)
    def setUp(self, strict_redis_class):
        self.mock_strict_redis = strict_redis_class.return_value

        self.task_callback = mock.Mock()

        self.client = Client(hostname="asdf", port=1234, db=0)

    def test_run_starts_requested_threads(self, mock_alive, mock_start, mock_event, mock_sleep):
        mock_alive.side_effect = BreakLoop()

        try:
            forking_runner.run(self.client, "some.queue", self.task_callback, 4)
        except BreakLoop:
            pass

        self.assertEqual(4, mock_start.call_count)
        self.assertEqual(4, mock_event.return_value.wait.call_count)
        mock_sleep.assert_called_with(1)

    def test_run_concurrency_defaults_to_1(self, mock_alive, mock_start, mock_event, mock_sleep):
        mock_alive.side_effect = BreakLoop()

        try:
            forking_runner.run(self.client, "some.queue", self.task_callback)
        except BreakLoop:
            pass

        self.assertEqual(1, mock_start.call_count)
        self.assertEqual(1, mock_event.return_value.wait.call_count)

    def test_run_reruns_threads_that_die(self, mock_alive, mock_start, mock_event, mock_sleep):
        mock_alive.side_effect = [True, False, False, True, BreakLoop()]

        try:
            forking_runner.run(self.client, "some.queue", self.task_callback, 4)
        except BreakLoop:
            pass

        self.assertEqual(6, mock_start.call_count)
        self.assertEqual(6, mock_event.return_value.wait.call_count)
