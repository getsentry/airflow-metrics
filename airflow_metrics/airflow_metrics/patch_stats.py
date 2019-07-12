import sys

from airflow import settings
from airflow.models import TaskInstance
from datadog import ThreadStats

from airflow_metrics.airflow_metrics.datadog_logger import DatadogStatsLogger
from airflow_metrics.utils.fn_utils import once
from airflow_metrics.utils.hook_utils import HookManager


@once
def patch_stats():
    org_logger = settings.Stats

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        def undo_patch(*args, **kwargs):
            logger.stop()
            settings.Stats = org_logger

        ti_run_raw_task_manager = HookManager(TaskInstance, '_run_raw_task')
        ti_run_raw_task_manager.register_post_hook(undo_patch)
        ti_run_raw_task_manager.wrap_method()

        def join(_, self, *args, **kwargs):
            self._flush_thread.join() # pylint: disable=protected-access

        threadstats_stop_manager = HookManager(ThreadStats, 'stop')
        threadstats_stop_manager.register_post_hook(join)
        threadstats_stop_manager.wrap_method()

    logger = DatadogStatsLogger()
    logger.start()
    settings.Stats = logger
