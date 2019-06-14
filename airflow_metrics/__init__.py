from airflow.plugins_manager import AirflowPlugin

from airflow_metrics.airflow_metrics import patch


patch()


class AirflowMetricsPlugin(AirflowPlugin):
    name = 'airflow_metrics'
