import sys

from datetime import datetime, timedelta
from threading import Thread
from time import sleep

import sqlalchemy

from airflow.models import TaskInstance
from airflow.settings import Stats
from airflow.utils.db import provide_session
from pytz import utc

from airflow_metrics.utils.fn_utils import once
from airflow_metrics.utils.fn_utils import capture_exception

@provide_session
def task_states(_since, session=None):
    states = (
        session.query(TaskInstance.state, sqlalchemy.func.count())
        .group_by(TaskInstance.state)
    )

    for state, count in states:
        if state is None:
            continue

        tags = {
            'state': state
        }
        Stats.gauge('task.state', count, tags=tags)


@provide_session
def bq_task_states(since, session=None):
    states = (
        session.query(TaskInstance.state, sqlalchemy.func.count())
        .filter(TaskInstance.operator == 'BigQueryOperator')
        .filter(TaskInstance.end_date > since)
        .group_by(TaskInstance.state)
    )

    for state, count in states:
        if state is None:
            continue

        tags = {
            'state': state
        }
        Stats.incr('task.state.bq', count, tags=tags)


def forever(funcs, sleep_time):
    passed = timedelta(seconds=sleep_time)

    def wrapped():
        while True:
            for func in funcs:
                since = datetime.utcnow() - passed
                func(utc.localize(since))
            sleep(sleep_time)
    return wrapped


@once
def patch_thread():
    try:
        if len(sys.argv) > 1 and sys.argv[1] == 'scheduler':
            funcs = [
                task_states,
                bq_task_states,
            ]
            thread = Thread(target=forever(funcs, 10))
            thread.daemon = True
            thread.start()
    except Exception as ex: # pylint: disable=broad-except
        capture_exception(ex)
