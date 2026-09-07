import argparse
import json
import importlib
import os
import sys
import multiprocessing
from .app import client, QUEUE_KEY, TASK_REGISTRY

CONCURRENCY = 5
DJANGO_SETTINGS_MODULE = None


def cwd_in_path():
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

def _init_django():
    """Must run in EVERY process that touches tasks -- parent and every child.
    Mirrors Celery's DjangoWorkerFixup, which fires per-process via
    the worker_process_init signal.
    """
    cwd_in_path()

    if "DJANGO_SETTINGS_MODULE" not in os.environ:
        raise RuntimeError(
            "DJANGO_SETTINGS_MODULE not set -- did run_worker() import "
            "the <app>.s_babes module first?"
        )

    import django
    django.setup()


def resolve(job):
    name = job["task"]
    if name not in TASK_REGISTRY:
        importlib.import_module(job["module"])  # populate registry on first use
    return TASK_REGISTRY.get(name)


def _child_loop(worker_id, job_queue, free_queue):
    """Runs inside each pre-forked child. Never touches Redis."""
    _init_django()
    print(f"[worker {worker_id} | PID {os.getpid()}] ready")
    free_queue.put(worker_id)

    while True:
        job = job_queue.get()  # blocks until parent sends work
        if job is None:
            break

        task_obj = resolve(job)
        if task_obj is None:
            print(f"[worker {worker_id}] Unknown task: {job['task']}")
        else:
            result = task_obj.func(*job["args"], **job["kwargs"])
            print(f"[worker {worker_id} | PID {os.getpid()}] Result: {result}")

        free_queue.put(worker_id)


def run_worker():
    parser = argparse.ArgumentParser(prog="s_babes-worker")
    parser.add_argument("-A", "--app", required=True, help="e.g. fiusdt")
    opts = parser.parse_args()

    cwd_in_path()

    config_upload = f"{opts.app}.s_babes"
    importlib.import_module(config_upload)

    print(f"Starting {CONCURRENCY} pre-forked workers (prefork), watching '{QUEUE_KEY}' ... (Ctrl+C to stop)")

    # one private inbox PER child (parent -> that specific child)
    job_queues = [multiprocessing.Queue() for _ in range(CONCURRENCY)]

    free_queue = multiprocessing.Queue()

    # PRE-fork: create every child upfront, before any job exists
    processes = []
    for i in range(CONCURRENCY):
        p = multiprocessing.Process(target=_child_loop, args=(i, job_queues[i], free_queue))
        p.start()
        processes.append(p)

    try:
        while True:
            _, raw = client.brpop(QUEUE_KEY)
            job = json.loads(raw)

            idle_worker_id = free_queue.get()
            job_queues[idle_worker_id].put(job)
    except KeyboardInterrupt:
        print("\nStopping workers...")
        for q in job_queues:
            q.put(None)
        for p in processes:
            p.join()
