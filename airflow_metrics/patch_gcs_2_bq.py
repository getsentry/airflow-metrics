from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryConnection
from airflow.settings import Stats
from airflow_metrics.utils.fn_utils import once
from airflow_metrics.utils.hook_utils import HookManager

import sys


def attach_cursor(return_value, context, *args, **kwargs):
    frame = sys._getframe(2)
    local_vars = frame.f_locals
    self = local_vars['self']
    try:
        if not isinstance(self, GoogleCloudStorageToBigQueryOperator):
            # we only want to monkey patch gcs2bq
            # make sure to pass the return value back
            return return_value
        self.__big_query_cursor__ = return_value
        return return_value
    finally:
        del frame
        del local_vars
        del self


def get_bq_job(ctx, self, *args, **kwargs):
    bq_cursor = self.__big_query_cursor__
    service = bq_cursor.service
    jobs = service.jobs()
    job = jobs.get(projectId=bq_cursor.project_id,
                   jobId=bq_cursor.running_job_id).execute()
    ctx['job'] = job


def bq_duration(ctx, self, *args, **kwargs):
    stats = ctx['job']['statistics']
    creation = int(stats['creationTime'])
    start = int(stats['startTime'])
    end = int(stats['endTime'])

    tags = {
        'dag': self.dag_id,
        'task': self.task_id,
        'operator': self.__class__.__name__,
    }
    Stats.timing('task.delay.gcs_to_bq', start - creation, tags=tags)
    Stats.timing('task.duration.gcs_to_bq', end - start, tags=tags)


def rows_copied(ctx, self, *args, **kwargs):
    rows = ctx['job']['statistics']['load']['outputRows']
    tags = {
        'dag': self.dag_id,
        'task': self.task_id,
        'operator': self.__class__.__name__,
    }
    Stats.gauge('task.upserted.gcs_to_bq', rows, tags=tags)


@once
def patch_gcs_2_bq():
    bq_connection_cursor_manager = HookManager(BigQueryConnection, 'cursor')
    bq_connection_cursor_manager.register_post_hook(attach_cursor)
    bq_connection_cursor_manager.post_process()

    gcs_to_bq_operator_execute_manager = HookManager(GoogleCloudStorageToBigQueryOperator, 'execute')
    gcs_to_bq_operator_execute_manager.register_post_hook(get_bq_job)
    gcs_to_bq_operator_execute_manager.register_post_hook(bq_duration)
    gcs_to_bq_operator_execute_manager.register_post_hook(rows_copied)
    gcs_to_bq_operator_execute_manager.wrap_method(skip_on_fail=True)
