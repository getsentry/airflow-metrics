from airflow.models import DagRun
from airflow.models import TaskInstance
from airflow.settings import Stats

from airflow_metrics.utils.event_utils import EventManager
from airflow_metrics.utils.fn_utils import once
from airflow_metrics.utils.hook_utils import HookManager


def dag_duration(target=None, **kwargs):
    if target.start_date and target.end_date:
        duration = (target.end_date - target.start_date).total_seconds()
        tags = {
            'dag': target.dag_id,
        }
        Stats.timing('dag.duration', duration * 1000, tags=tags)


def task_duration(target=None, **kwargs):
    if target.duration:
        tags = {
            'dag': target.dag_id,
            'task': target.task_id,
            'state': target.state,
            'operator': target.operator,
        }
        Stats.timing('task.duration', target.duration * 1000, tags=tags)


def normal_mode(*args, **kwargs):
    Stats.normal_mode()


@once
def patch_tasks():
    dag_run_after_update_manager = EventManager(DagRun, 'after_update')
    dag_run_after_update_manager.register_callback('end_date', dag_duration)

    task_instance_after_update_manager = EventManager(TaskInstance, 'after_update')
    task_instance_after_update_manager.register_callback('duration', task_duration)

    task_instance_run_manager = HookManager(TaskInstance, 'run')
    task_instance_run_manager.register_post_hook(normal_mode)
    task_instance_run_manager.wrap_method()
