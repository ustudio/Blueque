# Blueque #

![I'm afraid I just blue myself](http://25.media.tumblr.com/tumblr_lcrdvnhC9w1qfo5xwo1_500.jpg)

A simple task queue system optimized for very long running
tasks.

## Terminology ##

* *Queue*

	A list of tasks, of a given type, to be run. Multiple *queues* may
    be used to execute different types of tasks on different *nodes*.

* *Task*

	A piece of work to be done, including its parameters, current
    status, result (if done), etc.

* *Node*

	A computer running tasks from the queue.

* *Listener*

	A process running on a node, listening for new tasks on a single
    queue. The system does not support multiple listeners listening to
    the *same queue* on the *same node*.

* *Process*

	A process, running on a *node*, created by a *listener*, executing
    a *task*.

## API ##

### Client Connection ###

The `Blueque.Client` object stores the connection to the Blueque
system, and constructs the other objects necessary for interacting
with the system

```python
client = Blueque.Client(hostname, port, db)
```

### Queue ###

The `Blueque.queue.Queue` object provides the interface to a named
queue of tasks.

```python
queue = client.get_queue("some.queue")
```

#### `Queue.enqueue` ####

```python
task_id = queue.enqueue(parameters)
```

Returns the Task ID (a string) of the newly enqueued task

### Task ###

The task object provides a basic, read-only view of all the attributes
of a task.

```python
task = client.get_task(task_id)
```

There is a read-only attribute for all of the task attributes stored
in Redis (see below).

### Listener ###

The listener object provides the interface used to listen for new
tasks.

```python
listener = client.get_listener("some.queue")
```

#### `Listener.listen` ####

```python
def on_new_task(processor):
	print "starting task"

listener.listen(on_new_task)
```

Asynchronous function which takes a callback and never returns. The
callback will be called each time a task is available to be executed.


### Processor ###

A Processor object provides the interface used to update a task while
it is being processed. Processor objects will be passed to the
`Listener.listen` callback, and do not need to be constructed manually
for normal task execution operations.

If you need to construct one for administrative purposes, the
signature is:

```python
processor = Processor(listener_id, task_id, redis_queue)
```

#### `Processor.start` ####

```python
parameters = processor.start(pid)
```

Marks a task as being started, by the specified pid, and returns the
parameters of that task.

#### `Processor.complete` ####

```python
processor.complete("some result")
```

Marks a task as being complete, and stores the result.

#### `Processor.fail` ####

```python
processor.fail("some error")
```

Marks a task as having failed, and stores the error.

## Data Storage ##

Currently, the backend structure is Redis. Keys are prefixed with a
namespace, i.e. named "bluequeue_foo", so that other systems can use
the Redis DB for other things.

### Task Queue ###

`blueque_pending_tasks_[queue name]`

Stored in a `List`, accessed as a queue: new tasks are added via
`LPUSH`, tasks are removed for execution via `RPOP`. There is a `List`
for each task queue (channel). All that is stored in the `List` is a
task ID, which will be used to retrieve the task data.

### Listener Task List ###

`blueque_reserved_tasks_[queue name]_[listener ID]`

Stored in a `List`, this is used to keep track of which listeners are
running which tasks. Tasks should be atomically moved from the *Task
Queue* to the *Listener Task List* via `RPOPLPUSH`, so that they don't get
lost.

### Task Channel ###

Note: this will not be implemented in the first pass.

There is a Pub/Sub `Channel` for each task queue (channel). This can
be used by the listener client to listen for new tasks if the task queue
they were interested in was empty when they last checked for a task.

Messages should *not* include the task ID of the newly created task,
because listeners must be required to manually try to `LPOP` the task
off the task queue, so that only one work runs each task.

### Task List ###

`blueque_tasks_[queue name]`

There is a list of all the tasks in a queue, regardless of their
state. This is mostly used for introspection/management purposes.

### Task Data ###

`blueque_task_[task id]`

The actual data associated with a task will be stored as a hash, where
the key is built from the task ID (i.e. "bluequeue_task_NNNN"). Each
subsystem will be able to add its own fields to the hash.

Current fields are

* `status`

	One of: `pending`, `reserved`, `started`, `complete` or `failed`.

* `queue`

	The queue that the task is in (mostly just for debugging
    purposes).

* `parameters`

	String containing data to be passed to the process. Totally task
    specific.

* `result`

	String containing the result of the process; totally
    task-specific. Will not be set if the task hasn't completed yet.

* `error`

	String containing the error generated by the process. Set only if
    `status` is `failed`.

* `node`

	The listener ID of the node running the task. Will not be set if the
    task has not been started yet.

* `pid`

	The PID of the process running the task on the node. Will not be
    set if the task has not been started yet.

* `created`

	A floating point value containing the Python timestampe
    (`time.time()`) of the time when the task was first created
    (enqueued).

* `updated`

	A floating point value containing the Python timestamp
    (`time.time()`) of the last time any value in the task changed.

### Listeners ###

`blueque_listeners_[queue name]`

In order for the system to be easily introspected, the currently
active listeners will be stored in a Redis `Set`. Listeners are stored
by their `LISTENER ID`, which must be `[hostname]_[pid]`.

Note that this means that all hosts in the system must have unique
names.

### Queues ###

`blueque_queues`

There is a `Sorted Set` containing the names of all the queues, where
the score of the set is the number of nodes listening to that set.

When a node comes online, it increments the score by 1; when a node
goes offline (cleanly) it increments the score by -1. Every time a
task is enqueued, the score should be incremented by 0, so that a
queue with tasks, but no listeners, still shows up in the set.

## Task Workflow ##

Tasks are executed via this workflow. Note that any `HMSET` call which
modifies a `blueque_task_[TASK ID]` should be assumed to also set the
`updated` field of the task.

### Submission ###

Tasks should be submitted by creating a UUID, [TID], JSON encoding the
parameters, [PARAMS], and then executing:

```
MULTI
HMSET blueque_task_[TASK ID] status pending queue [QUEUE] parameters [PARAMS]
ZINCRBY blueque_queues 0 [QUEUE]
LPUSH blueque_pending_tasks_[QUEUE] [TASK ID]
EXEC
```

### Node Task Pop ###

Nodes should pop a task off the queue and then set the status of the
task to `started`, and set the `node` field of the task.

If no task is popped off the queue, the Node should wait for a new
task notification. Ideally, this will be via Pub/Sub, but, at first,
we can do it by polling.

Tasks are popped using the following commands.

```
RPOPLPUSH blueque_pending_tasks_[QUEUE] [NODE TASKS]
```

```
HMSET blueque_task_[TASK ID] status reserved node [NODE]
```

Note that these two commands cannot be executed atomically because the
second depends on the first, and, even with Lua scripting, that cannot
be done atomically and safely. Therefore, there is a chance that a
task is popped off the pending queue, but its record is not
updated. This can be detected if a task is in a node's queue, but has
a status of `pending`.

### Task Queue Channel Message ###

Note: We will not implement this in the first pass. Listeners will just
poll once every few seconds.

If a listener receives a message via a Pub/Sub channel that a queue has
a task in it, it should try to atomically pop a task off that channel
(see above). If it does get a task, it should unsubscribe from the
channel; if it does not (i.e. another listener got the task) it should
remain subscribed.

### Task Started ###

When a process starts executing a task on a node, it should update the
task to indicate that, and also add itself to a set of all active
tasks:

```
MULTI
SADD blueque_started_tasks_[QUEUE] "[LISTENER ID] [PID] [TASK ID]"
HMSET blueque_task_[TASK ID] status started pid [PID]
HGET blueque_task_[TASK ID] parameters
EXEC
```

Note that this assumes that the process is told what task to execute,
rather than pulling it off the node's task list.

### Task Completed Successfully ###

If a task completes successfully, it should set the `status` field of
the task to `succeeded` and set the `result` field to the
JSON-serialized result of the task, as a single atomic transaction.

```
MULTI
LREM blueque_reserved_tasks_[QUEUE]_[LISTENER ID] [TASK ID]
LREM blueque_started_tasks_[QUEUE] 1 "[LISTENER ID] [PID] [TASK ID]"
HMSET blueque_task_[TASK ID] status complete result [RESULT]
LPUSH blueque_complete_tasks_[QUEUE] [TASK ID]
EXEC
```

### Task Failed ###

If a task fails for any reason (process function raises an exception,
or some monitoring process determines that the process fails more
catastrophically), the process that detects the error should set the
`status` field of the task to `failed` and the `error` field to a
JSON-serialized description of the error (see above).

```
MULTI
LREM blueque_reserved_tasks_[QUEUE]_[LISTENER ID] [TASK ID]
LREM blueque_started_tasks_[QUEUE] 1 "[LISTENER ID] [PID] [TASK ID]"
HMSET blueque_task_[TASK ID] status failed error [ERROR]
LPUSH blueque_failed_tasks_[QUEUE] [TASK ID]
EXEC
```
