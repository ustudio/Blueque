# Blueque #

![I'm afraid I just blue myself](http://25.media.tumblr.com/tumblr_lcrdvnhC9w1qfo5xwo1_500.jpg)

A simple task queue system optimized for very long running
tasks.

## Data Storage ##

Currently, the backend structure is Redis. Keys should probably be
prefixed with a namespace, i.e. "bluequeue_foo", so that other systems
can use the Redis DB for other things.

### Task Queue ###

Stored in a `List`, accessed as a queue: new tasks are added via
`RPUSH`, tasks are removed for execution via `LPOP`. There is a `List`
for each task queue (channel). All that is stored in the `List` is a
task ID, which will be used to retrieve the task data.

### Task Channel ###

There is a Pub/Sub `Channel` for each task queue (channel). This can
be used by the worker client to listen for new tasks if the task queue
they were interested in was empty when they last checked for a task.

Messages should *not* include the task ID of the newly created task,
because workers must be required to manually try to `LPOP` the task
off the task queue, so that only one work runs each task.

### Task Data ###

The actual data associated with a task will be stored as a hash, where
the key is built from the task ID (i.e. "bluequeue_task_NNNN"). Each
subsystem will be able to add its own fields to the hash.

Current fields are

* `status`

	One of: `pending`, `started`, `succeeded` or `failed`.

* `parameters`

	JSON serialized parameters which will be passed to the worker
    function; totally task-specific.

* `result`

	JSON serialized result of the task, as returned by the worker
    function; totally task-specific. Will not be set if the task
    hasn't completed yet.

* `worker`

	The worker ID and the PID of the worker process on that worker,
    separated by a space. Will not be set if the task has not been
    started yet.

* `error`

	JSON serialized description of the error. Set only if `status` is
    `failed`.

	TODO: Determine basic structure of this field.

### Workers ###

In order for the system to be easily introspected, the currently
active workers will be stored in Redis. Each will be stored as a
simple string, where the key is built from the ID of the worker
(i.e. "bluequeue_worker_NNNN") and the value is a JSON serialized
description of the worker.

TODO: should the "ID" be the hostname or IP address of the worker, so
that they are more easily identified?

TODO: should workers be required to post a "heartbeat" back to their
worker key? If so, we could monitor that they are alive, but we would
need to decide how they post back: would the worker function be
responsible for posting back while it is running?

## Task Workflow ##

Tasks are executed via this workflow.

### Submission ###

Tasks are submitted by creating a Task record, with a UUID, a
status of `pending` and whatever `parameters` were specified, and
appending the task ID to appropriate channel via `RPUSH`. These
operations should be atomically executed via a `Transaction`.

### Worker Start ###

When a Worker starts, it should create a `Worker` record of itself,
filling in the appropriate data. It should then attempt to pull a task
off the task queue it is interested in.

### Worker Task Pop ###

Workers should pop a task off the queue and then set the status of the
task to `started`, ideally as an atomic transaction (might need to be
done via a Lua script).

If a worker ever attempts to pop a task off the queue, and there is no
task, it should subscribe to the queue's pub/sub channel.

### Task Queue Channel Message ###

If a worker receives a message via a Pub/Sub channel that a queue has
a task in it, it should try to atomically pop a task off that channel
(see above). If it does get a task, it should unsubscribe from the
channel; if it does not (i.e. another worker got the task) it should
remain subscribed.

### Task Completed Successfully ###

If a task completes successfully, it should set the `status` field of
the task to `succeeded` and set the `result` field to the
JSON-serialized result of the task, as a single atomic transaction.

### Task Failed ###

If a task fails for any reason (worker function raises an exception,
or some monitoring process determines that the worker fails more
catastrophically), the process that detects the error should set the
`status` field of the task to `failed` and the `error` field to a
JSON-serialized description of the error (see above).
