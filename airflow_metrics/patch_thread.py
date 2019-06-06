from airflow.models import TaskInstance
from airflow.settings import Stats
from airflow.utils.db import provide_session
from datetime import datetime, timedelta
from pytz import utc
from threading import Thread
from time import sleep

import sqlalchemy
import sys


@provide_session
def task_states(since, session=None):
    states = (
        session.query(TaskInstance.state, sqlalchemy.func.count())
        .group_by(TaskInstance.state)
    )

    for state, count in states:
        Stats.gauge('dag.task.state.{}'.format(state), count)


@provide_session
def bq_task_states(since, session=None):
    states = (
        session.query(TaskInstance.state, sqlalchemy.func.count())
        .filter(TaskInstance.operator=='BigQueryOperator')
        .filter(TaskInstance.end_date>since)
        .group_by(TaskInstance.state)
    )

    total = 0
    for state, count in states:
        if state is None:
            continue

        Stats.incr('dag.task.bq.state.{}'.format(state), count)
        total += count

    Stats.incr('dag.task.bq.count', total)


def forever(fns, sleep_time):
    passed = timedelta(seconds=sleep_time)

    def fn():
        while True:
            for fn in fns:
                since = datetime.utcnow() - passed
                fn(utc.localize(since))
            sleep(sleep_time)
    return fn


def patch_thread():
    if sys.argv[1] == 'scheduler':
        fns = [
            task_states,
            bq_task_states,
        ]
        thread = Thread(target=forever(fns, 10))
        thread.start()
