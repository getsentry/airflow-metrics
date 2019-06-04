from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryCursor
from airflow.settings import Stats
from airflow_metrics.utils.hook_utils import HookManager


def get_bq_job(ctx, self, *args, **kwargs):
    bq_cursor = self.bq_cursor
    service = bq_cursor.service
    jobs = service.jobs()
    job = jobs.get(projectId=bq_cursor.project_id,
                   jobId=bq_cursor.running_job_id).execute()
    ctx['job'] = job


def bq_upserted(ctx, self, *args, **kwargs):
    query_stats = ctx['job']['statistics']['query']['queryPlan']

    all_queries = set()
    upstream_queries = set()

    for stat in query_stats:
        all_queries.add(stat['id'])
        upstream_queries.update(set(stat.get('inputStages', [])))

    final_queries = all_queries - upstream_queries
    written = 0

    for stat in query_stats:
        if stat['id'] not in final_queries:
            continue

        written += int(stat['recordsWritten'])

    metric_id = 'dag.{}.{}.bq.upserted'.format(self.dag_id,
                                               self.task_id)
    Stats.gauge(metric_id, written)


def patch_bq():
    bq_operator_execute_manager = HookManager(BigQueryOperator, 'execute')
    bq_operator_execute_manager.register_post_hook(get_bq_job)
    bq_operator_execute_manager.register_post_hook(bq_upserted)
    bq_operator_execute_manager.wrap_method(skip_on_fail=True)
