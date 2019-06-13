from airflow import settings

from airflow_metrics.airflow_metrics.datadog_logger import DatadogStatsLogger
from airflow_metrics.utils.fn_utils import once


@once
def patch_stats():
    logger = DatadogStatsLogger()
    logger.start()
    settings.Stats = logger
