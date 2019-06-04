from airflow.models import DagRun
from airflow.models import TaskInstance
from airflow_metrics.utils.event_utils import EventManager
from airflow_metrics.utils.fn_utils import once
from airflow.settings import Stats


def dag_duration(target=None, new=None, old=None):
    if target.start_date and target.end_date:
        metric_id = 'dag.{}.{}.duration'.format(target.dag_id,
                                                target.state)
        duration = (target.end_date - target.start_date).total_seconds()

        Stats.timing(metric_id, duration * 1000)


def task_duration(target=None, new=None, old=None):
    if target.duration:
        metric_id = 'dag.{}.{}.{}.duration'.format(target.dag_id,
                                                   target.task_id,
                                                   target.state)

        Stats.timing(metric_id, target.duration * 1000)


@once
def patch_tasks():
    dag_run_after_update_manager = EventManager(DagRun, 'after_update')
    dag_run_after_update_manager.register_callback('end_date', dag_duration)

    task_instance_after_update_manager = EventManager(TaskInstance, 'after_update')
    task_instance_after_update_manager.register_callback('duration', task_duration)
