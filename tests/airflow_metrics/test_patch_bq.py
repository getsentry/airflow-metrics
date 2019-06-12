from airflow_metrics.airflow_metrics.patch_bq import bq_duration
from airflow_metrics.airflow_metrics.patch_bq import bq_upserted
from airflow_metrics.airflow_metrics.patch_bq import get_bq_job
from unittest import TestCase
from unittest.mock import Mock
from unittest.mock import patch


class TestGetBqJob(TestCase):
    def setUp(self):
        self.self = Mock()
        self.ctx = {'success': True}

    def test_job_created(self):
        assert 'job' not in self.ctx
        get_bq_job(self.ctx, self.self)
        assert self.ctx['job']


class TestBqUpserted(TestCase):
    def setUp(self):
        self.self = Mock()
        self.self.dag_id = 'dag-id'
        self.self.task_id = 'task-id'
        self.self.__class__.__name__ = 'MockOperator'
        self.ctx = {
            'job': {
                'statistics': {
                    'query': {
                        'queryPlan': [
                            {
                                'id': '0',
                                'inputStages': [],
                                'recordsWritten': 0,
                            },
                            {
                                'id': '1',
                                'inputStages': ['0'],
                                'recordsWritten': 1,
                            },
                            {
                                'id': '2',
                                'inputStages': ['0'],
                                'recordsWritten': 2,
                            },
                            {
                                'id': '3',
                                'inputStages': ['1'],
                                'recordsWritten': 3,
                            },
                            {
                                'id': '4',
                                'inputStages': ['3'],
                                'recordsWritten': 4,
                            },
                            {
                                'id': '5',
                                'inputStages': ['2', '3'],
                                'recordsWritten': 5,
                            },
                        ],
                    },
                },
            },
        }

    def test_gauge(self):
        with patch('airflow_metrics.airflow_metrics.patch_bq.Stats') as Stats:
            bq_upserted(self.ctx, self.self)
            assert Stats.gauge.call_args == (
                ('task.upserted.bq', 9),
                {'tags': {'dag': 'dag-id', 'task': 'task-id', 'operator': 'MockOperator'}},
            )


class TestBqDuration(TestCase):
    def setUp(self):
        self.self = Mock()
        self.self.dag_id = 'dag-id'
        self.self.task_id = 'task-id'
        self.self.__class__.__name__ = 'MockOperator'
        self.ctx = {
            'job': {
                'statistics': {
                    'creationTime': 0,
                    'startTime': 1,
                    'endTime': 3,
                },
            },
        }

    def test_timing(self):
        with patch('airflow_metrics.airflow_metrics.patch_bq.Stats') as Stats:
            bq_duration(self.ctx, self.self)
            assert Stats.timing.call_args_list == [
                (
                    ('task.delay.bq', 1),
                    {'tags': {'dag': 'dag-id', 'task': 'task-id', 'operator': 'MockOperator'}},
                ),
                (
                    ('task.duration.bq', 2),
                    {'tags': {'dag': 'dag-id', 'task': 'task-id', 'operator': 'MockOperator'}},
                ),
            ]
