from airflow.settings import Stats
from sqlalchemy import inspect


def report_state(target=None, new=None, old=None):
    if new:  # can be None
        Stats.incr('dag.task.state.{}'.format(new))

    if old:  # can be None
        Stats.decr('dag.task.state.{}'.format(old))
