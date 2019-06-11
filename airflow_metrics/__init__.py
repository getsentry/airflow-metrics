from airflow_metrics.utils.fn_utils import once


@once
def patch():
    from airflow_metrics.airflow_metrics.patch_stats import patch_stats
    patch_stats()

    from airflow_metrics.airflow_metrics.patch_tasks import patch_tasks
    patch_tasks()

    from airflow_metrics.airflow_metrics.patch_thread import patch_thread
    patch_thread()

    from airflow_metrics.airflow_metrics.patch_bq import patch_bq
    patch_bq()

    from airflow_metrics.airflow_metrics.patch_gcs_2_bq import patch_gcs_2_bq
    patch_gcs_2_bq()

    from airflow_metrics.airflow_metrics.patch_requests import patch_requests
    patch_requests()
