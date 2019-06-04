from airflow.models import TaskInstance
from airflow_metrics.utils.event_utils import EventManager
from airflow_metrics.utils.fn_utils import once
from airflow.settings import Stats


def report_duration(target=None, new=None, old=None):
    if target.duration:
        metric_id = 'dag.{}.{}.{}.duration'.format(target.dag_id,
                                                   target.task_id,
                                                   target.state)

        Stats.timing(metric_id, target.duration * 1000)


@once
def patch_tasks():
    task_instance_after_update_manager = EventManager(TaskInstance, 'after_update')
    task_instance_after_update_manager.register_callback('duration', report_duration)
