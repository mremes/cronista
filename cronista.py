import time
import datetime
import uuid
from datetime import timedelta
from enum import Enum
from functools import partial
import subprocess
import random
import math
import collections

task_threshold = 2
utcnow = partial(datetime.datetime.utcnow)
rnd = random.Random()


class States(Enum):
    NONE = "none"
    ERROR = "error"
    FAILURE = "failure"
    SUCCESS = "success"


def random_task(dt_now: datetime, cmd: str, secs_range: int=30):
    return 'bash', cmd, dt_now + timedelta(seconds=random.randint(0, secs_range)), "Task %s" % uuid.uuid4()


def wait_cycle(seconds: int):
    print('waiting for %s seconds' % seconds)
    time.sleep(seconds)


def executables(iterable: collections.Iterable):
    l = [i for i in iterable]
    for k in l:
        print('--------------------------')
        yield k
        print('--------------------------')


def do_exit(prcs, wait_time):
    if len(prcs) >= task_threshold:
        wait_cycle(seconds=wait_time)
        return True
    return False


tasks = [random_task(utcnow(), 'echo foofoo', 10),
         random_task(utcnow(), 'echo faafaa', 60),
         random_task(utcnow(), 'echo faafaa', 20),
         random_task(utcnow(), 'echo faafaa', 3)]
processes = []


def handle_tasks():
    for task in executables(tasks):
        ex, cmd, date, msg = task
        if do_exit(processes, 5):
            break
        if date <= now:
            print('date: ', date), print('now: ', now)
            p = subprocess.Popen(cmd.split(' '))
            print(msg, 'executed at %s with execution date %s' % (now, date))
            p.__hash__ = int(time.time()*(10**6))
            tasks.remove(task)
            processes.append((p, task))
        else:
            print('no tasks to schedule.')
        wait_cycle(seconds=1)


def handle_processes():
    for pr in executables(processes):
        r = math.floor(rnd.uniform(0, 20))
        print('random %s for %s' % (r, pr[1][3]))
        if r > 3:
            continue
        processes.remove(pr)
    wait_cycle(seconds=1)


while (len(tasks) + len(processes)) > 0:
    now = utcnow()
    if len(tasks) > 0:
        handle_tasks()
    if (len(processes)) > 0:
        handle_processes()
    print('%s tasks and %s active processes' % (len(tasks), len(processes)))

print('scheduler ready with tasks.')
