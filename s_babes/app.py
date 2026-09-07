import json
import redis

QUEUE_KEY = "s_babes:queue"

TASK_REGISTRY = {}

client = redis.Redis(host="localhost", port=6379, db=0)


class Task:
    def __init__(self, func):
        self.func = func
        self.module = func.__module__
        self.name = f"{func.__module__}.{func.__name__}"
        TASK_REGISTRY[self.name] = self

    def delay(self, *args, **kwargs):
        job = {
            "task": self.name,
            "module": self.module,
            "func": self.func.__name__,
            "args": args,
            "kwargs": kwargs,
        }
        client.lpush(QUEUE_KEY, json.dumps(job))
        print(f"Queued '{self.name}' args={args}")

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


def task(func):
    return Task(func)
