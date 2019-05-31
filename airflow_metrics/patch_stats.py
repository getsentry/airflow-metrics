from airflow import settings
from airflow_metrics.airflow_metrics.datadog_logger import DatadogStatsLogger


def patch_stats():
    # TODO: called twice for some reason
    settings.Stats = DatadogStatsLogger() # tested on apache-airflow==1.10.3
