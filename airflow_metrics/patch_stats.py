from airflow import settings
from airflow_metrics.airflow_metrics.datadog_logger import DatadogStatsLogger
from airflow_metrics.utils.fn_utils import once


@once
def patch_stats():
    settings.Stats = DatadogStatsLogger() # tested on apache-airflow==1.10.3
