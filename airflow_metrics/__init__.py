from airflow_metrics.airflow_metrics.patch_stats import patch_stats


def patch():
    # TODO: called twice for some reason
    patch_stats()
