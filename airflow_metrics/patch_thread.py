from airflow.models import TaskInstance
from airflow.settings import Stats
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin
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
        if state is None:
            continue

        tags = { 'state': state }
        Stats.gauge('task.state', count, tags=tags)


@provide_session
def bq_task_states(since, session=None):
    states = (
        session.query(TaskInstance.state, sqlalchemy.func.count())
        .filter(TaskInstance.operator=='BigQueryOperator')
        .filter(TaskInstance.end_date>since)
        .group_by(TaskInstance.state)
    )

    for state, count in states:
        if state is None:
            continue

        tags = { 'state': state }
        Stats.incr('task.state.bq', count, tags=tags)


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
    try:
        if sys.argv[1] == 'scheduler':
            fns = [
                task_states,
                bq_task_states,
            ]
            thread = Thread(target=forever(fns, 10))
            thread.start()
    except Exception as e:
        # if any of this throws an error, then we're not in any
        # state to start this metrics thread, so give up now
        log = LoggingMixin().log
        msg = 'The following error occured starting the metrics thread! Skipping...\n{}'
        log.warn(msg.format(str(e)))
