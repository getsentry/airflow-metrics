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
        from airflow_metrics.airflow_metrics.patch_tasks import patch_tasks
        patch_tasks()

    if enabled('thread'):
        from airflow_metrics.airflow_metrics.patch_thread import patch_thread
        patch_thread()

    if enabled('bq'):
        from airflow_metrics.airflow_metrics.patch_bq import patch_bq
        patch_bq()

    if enabled('gcs_to_bq'):
        from airflow_metrics.airflow_metrics.patch_gcs_2_bq import patch_gcs_2_bq
        patch_gcs_2_bq()

    if enabled('requests'):
        from airflow_metrics.airflow_metrics.patch_requests import patch_requests
        patch_requests()
