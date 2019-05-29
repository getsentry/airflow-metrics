from airflow_metrics.airflow_metrics.patch_stats import patch_stats
from airflow_metrics.utils.fn_utils import once


@once
def patch():
    patch_stats()
