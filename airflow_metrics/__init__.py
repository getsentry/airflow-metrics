from airflow_metrics.utils.fn_utils import once


@once
def patch():
    from airflow_metrics.airflow_metrics.patch_stats import patch_stats
    patch_stats()

    from airflow_metrics.airflow_metrics.patch_tasks import patch_tasks
    patch_tasks()
