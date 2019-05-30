from airflow.models import TaskInstance
from airflow_metrics.airflow_metrics.task_state import report_state
from airflow_metrics.utils.event_utils import EventManager
from airflow_metrics.utils.fn_utils import once


@once
def patch_tasks():
    task_instance_after_insert_manager = EventManager(TaskInstance, 'after_insert')
    task_instance_after_insert_manager.register_callback('state', report_state)

    task_instance_after_update_manager = EventManager(TaskInstance, 'after_update')
    task_instance_after_update_manager.register_callback('state', report_state)
