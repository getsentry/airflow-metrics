from functools import wraps

from airflow.contrib.hooks.bigquery_hook import BigQueryConnection
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.settings import Stats

from airflow_metrics.utils.fn_utils import get_calling_operator
from airflow_metrics.utils.fn_utils import once
from airflow_metrics.utils.hook_utils import HookManager


@HookManager.success_only
def attach_cursor(ctx, *args, **kwargs):
    operator = get_calling_operator()
    if isinstance(operator, GoogleCloudStorageToBigQueryOperator):
        operator.__big_query_cursor__ = ctx['return']


def has_cursor(func):
    @wraps(func)
    def wrapped(ctx, self, *args, **kwargs):
        if not hasattr(self, '__big_query_cursor__'):
            return None
        return func(ctx, self, *args, **kwargs)
    return wrapped


@HookManager.success_only
@has_cursor
def get_bq_job(ctx, self, *args, **kwargs):
    bq_cursor = self.__big_query_cursor__
    service = bq_cursor.service
    jobs = service.jobs()
    job = jobs.get(projectId=bq_cursor.project_id,
                   jobId=bq_cursor.running_job_id).execute()
    ctx['job'] = job


@HookManager.success_only
@has_cursor
def bq_upserted(ctx, self, *args, **kwargs):
    rows = ctx['job']['statistics']['load']['outputRows']
    tags = {
        'dag': self.dag_id,
        'task': self.task_id,
        'operator': self.__class__.__name__,
    }
    Stats.gauge('task.upserted.gcs_to_bq', rows, tags=tags)


@HookManager.success_only
@has_cursor
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


@once
def patch_gcs_2_bq():
    bq_connection_cursor_manager = HookManager(BigQueryConnection, 'cursor')
    bq_connection_cursor_manager.register_post_hook(attach_cursor)
    bq_connection_cursor_manager.wrap_method()

    gcs_to_bq_operator_execute_manager = \
            HookManager(GoogleCloudStorageToBigQueryOperator, 'execute')
    gcs_to_bq_operator_execute_manager.register_post_hook(get_bq_job)
    gcs_to_bq_operator_execute_manager.register_post_hook(bq_upserted)
    gcs_to_bq_operator_execute_manager.register_post_hook(bq_duration)
    gcs_to_bq_operator_execute_manager.wrap_method()
