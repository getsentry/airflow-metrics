from airflow.models import TaskInstance
from airflow.settings import Stats
from airflow.utils.db import provide_session
from threading import Thread
from time import sleep

import sqlalchemy
import sys


@provide_session
def report_states(session=None):
    while True:
        states = (
            session.query(TaskInstance.state, sqlalchemy.func.count())
            .group_by(TaskInstance.state)
        )

        for state, count in states:
            Stats.gauge('dag.task.state.{}'.format(state), count)

        sleep(10)


def patch_thread():
    if sys.argv[1] == 'webserver':
        thread = Thread(target=report_states)
        thread.start()
        thread.join()
