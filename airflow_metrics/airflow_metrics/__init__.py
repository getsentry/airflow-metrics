from airflow import configuration as conf

from airflow_metrics.utils.fn_utils import once
from airflow_metrics.utils.fn_utils import enabled
from airflow_metrics.utils.fn_utils import swallow_error


@once
@swallow_error
def patch():
    if not enabled():
        return

    from airflow_metrics.airflow_metrics.patch_stats import patch_stats
    patch_stats()

    if enabled('tasks'):
        monkey_patch_tasks()

    if enabled('thread'):
        monkey_patch_thread()

    if enabled('bq'):
        monkey_patch_bq()

    if enabled('gcs_to_bq'):
        monkey_patch_gcs_2_bq()

    if enabled('requests'):
        monkey_patch_requests()


@swallow_error
def monkey_patch_tasks():
    from airflow_metrics.airflow_metrics.patch_tasks import patch_tasks
    patch_tasks()


@swallow_error
def monkey_patch_thread():
    from airflow_metrics.airflow_metrics.patch_thread import patch_thread
    patch_thread()


@swallow_error
def monkey_patch_bq():
    from airflow_metrics.airflow_metrics.patch_bq import patch_bq
    patch_bq()


@swallow_error
def monkey_patch_gcs_2_bq():
    from airflow_metrics.airflow_metrics.patch_gcs_2_bq import patch_gcs_2_bq
    patch_gcs_2_bq()


@swallow_error
def monkey_patch_requests():
    from airflow_metrics.airflow_metrics.patch_requests import patch_requests
    patch_requests()
