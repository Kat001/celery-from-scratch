# s_babes

A minimal Celery-style distributed task queue: a Redis broker, a pre-forking
worker pool, and Django integration. Written to make the moving parts of a task
queue visible rather than to be production infrastructure.

## Requirements

- Python 3.8+
- Redis (broker)
- Django (the worker requires it — see [Limitations](#limitations))

## Install

```bash
pip install /path/to/s_babes
```

Two console scripts are installed:

| Command | Purpose |
| --- | --- |
| `s_babes` | trivial hello-world CLI (`s_babes/cli.py`) |
| `s_babes-worker` | the worker |

## Quickstart

### 1. Add a config module to your Django project

Next to `settings.py`, create `<project>/s_babes.py`. This is the analogue of
Celery's `celery.py` — the worker imports it before anything else, and its only
job is to point Django at your settings:

```python
# celery_mock/s_babes.py
import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_mock.settings")
```

### 2. Define tasks

```python
# accounts/tasks.py
from s_babes import task
from django.contrib.auth.models import User

@task
def add(x, y):
    return x + y

@task
def count_users():
    return User.objects.count()
```

Task modules must be **import-safe**: define tasks at module level and nothing
else. The worker imports them, so any top-level side effect (a `print`, a
`.delay()` call, an HTTP request) runs a second time inside the worker.

### 3. Start the worker

From your project root — the directory containing `manage.py`:

```bash
PYTHONUNBUFFERED=1 s_babes-worker -A celery_mock
```

```
Starting 5 pre-forked workers (prefork), watching 's_babes:queue' ... (Ctrl+C to stop)
[worker 0 | PID 32362] ready
[worker 1 | PID 32363] ready
...
```

`PYTHONUNBUFFERED=1` matters whenever stdout is not a terminal. Child output is
block-buffered otherwise, so logs are invisible until the buffer fills and are
lost entirely if the worker is killed.

### 4. Queue work

```python
from accounts.tasks import add, count_users

add.delay(10, 20)
count_users.delay()
```

```
[worker 1 | PID 32363] Result: 30
[worker 4 | PID 32366] Result: 1
```

Calling the task directly — `add(10, 20)` — bypasses the queue and runs inline
in the caller.

## How it works

```
producer process                  Redis                worker parent            5 child processes
────────────────                  ─────                ─────────────            ─────────────────
@task registers                                        imports <app>.s_babes    each runs django.setup()
add.delay(10, 20) ──── LPUSH ──▶ s_babes:queue         BRPOP ──▶ dispatch ────▶ import job["module"]
                                                       to an idle child         look up job["task"]
                                                                                call it
```

**Only the name crosses the wire.** The payload is JSON:

```json
{"task": "accounts.tasks.add", "module": "accounts.tasks", "func": "add", "args": [10, 20], "kwargs": {}}
```

**`TASK_REGISTRY` is per-process.** It is a module-level dict in `s_babes/app.py`,
so every process builds its own and nothing about it is shared. The producer
fills it by importing its task modules; each worker child starts empty and fills
it lazily — `resolve()` (`worker.py:35`) sees an unknown name, imports the
payload's `module`, which re-runs the `@task` decorators in *that* process, and
the lookup then succeeds.

The registry is also an allow-list: a job naming a function that was never
decorated with `@task` is refused rather than invoked.

**Prefork.** All 5 children are spawned before any job arrives. The parent owns
the only Redis connection; children never touch Redis. Dispatch is via one
private `multiprocessing.Queue` per child, plus a shared `free_queue` that
children use to announce they are idle — so the parent hands each job to a child
it knows is free.

**Django is initialised per-process.** `django.setup()` runs in every child
(`_init_django`, `worker.py:18`), not just the parent, because each process has
its own memory, app registry, and DB connections. This mirrors Celery's
`worker_process_init` signal.

## Configuration

Mostly hardcoded at present:

| Setting | Value | Location |
| --- | --- | --- |
| Redis | `localhost:6379` db `0` | `app.py:8` |
| Queue key | `s_babes:queue` | `app.py:4` |
| Concurrency | `5` | `worker.py:9` |

## CLI

```
s_babes-worker -A <app>
```

`-A/--app` is required. `s_babes-worker -A celery_mock` imports
`celery_mock.s_babes`, which is what sets `DJANGO_SETTINGS_MODULE`.

The worker adds the current working directory to `sys.path`, because console
scripts otherwise only get the `bin/` directory — run it from your project root.

## Limitations

Known and verified; this is a teaching implementation, not a Celery replacement.

- **A task that raises kills its child.** `_child_loop` has no `try`/`except`
  around the call, so an exception ends the loop and the process. That child
  never returns its id to `free_queue`, so pool capacity drops permanently.
  After 5 failures the parent blocks forever in `free_queue.get()` and the
  worker is deadlocked with jobs stuck in Redis.
- **Jobs are lost on crash.** `BRPOP` removes the job before it is dispatched,
  and there is no processing list, ack, or retry. A job in flight when a child
  dies is gone.
- **No result backend.** `.delay()` returns `None`; results are printed by the
  worker and discarded.
- **Django is mandatory.** `_init_django` raises if `DJANGO_SETTINGS_MODULE` is
  unset, so the worker cannot run for a plain Python project.
- **The Redis client is built at import time** (`app.py:8`), so importing
  `s_babes` constructs one whether or not you queue anything.
- No task timeouts, rate limits, retries, scheduling, routing, or multiple
  queues.

## Layout

```
s_babes/
├── __init__.py   # public API: task, Task, say_hello
├── app.py        # Task, the @task decorator, TASK_REGISTRY, Redis client
├── worker.py     # prefork pool, Django bootstrap, dispatch loop
├── cli.py        # `s_babes` hello-world command
└── core.py       # say_hello
```
