from unittest import TestCase
from unittest.mock import Mock
from unittest.mock import patch

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from airflow_metrics.airflow_metrics.patch_gcs_2_bq import attach_cursor
from airflow_metrics.airflow_metrics.patch_gcs_2_bq import bq_duration
from airflow_metrics.airflow_metrics.patch_gcs_2_bq import bq_upserted
from airflow_metrics.airflow_metrics.patch_gcs_2_bq import get_bq_job
from airflow_metrics.airflow_metrics.patch_gcs_2_bq import has_cursor
from airflow_metrics.utils.hook_utils import HookManager
from tests.utility import mockfn


class TestAttachCursor(TestCase):
    def test_successfully_attach(self):
        class TestClass():
            def test_method(self):
                return mock

        class TestOperator(GoogleCloudStorageToBigQueryOperator):
            def execute(self, context):
                test_object = TestClass()
                test_object.test_method()

        test_method_manager = HookManager(TestClass, 'test_method')
        test_method_manager.register_post_hook(attach_cursor)
        test_method_manager.wrap_method()

        mock = Mock()
        operator = TestOperator(task_id='task-id', bucket=None, source_objects=None,
                                destination_project_dataset_table=None)
        operator.execute(None)
        assert operator.__big_query_cursor__ == mock # pylint: disable=no-member


class TestHasCursor(TestCase):
    def test_does_have_cursor(self):
        class TestClass():
            pass
        ctx = {}
        this = TestClass()
        this.__big_query_cursor__ = Mock() # pylint: disable=attribute-defined-outside-init

        @mockfn
        def fn_mock(*args, **kwargs):
            pass
        fn = has_cursor(fn_mock)

        assert not fn_mock.called
        fn(ctx, this)
        assert fn_mock.called

    def test_doesnt_have_cursor(self):
        class TestClass():
            pass
        ctx = {}
        this = TestClass()

        @mockfn
        def fn_mock(*args, **kwargs):
            pass
        fn = has_cursor(fn_mock)

        assert not fn_mock.called
        fn(ctx, this)
        assert not fn_mock.called


class TestGetBqJob(TestCase):
    def setUp(self):
        self.self = Mock()
        self.self.__big_query_cursor__ = Mock()
        self.ctx = {
            'success': True,
        }

    def test_job_created(self):
        assert 'job' not in self.ctx
        get_bq_job(self.ctx, self.self)
        assert self.ctx['job']


class TestBqUpserted(TestCase):
    def setUp(self):
        self.self = Mock()
        self.self.__big_query_cursor__ = Mock()
        self.self.dag_id = 'dag-id'
        self.self.task_id = 'task-id'
        self.self.__class__.__name__ = 'MockOperator'
        self.ctx = {
            'success': True,
            'job': {
                'statistics': {
                    'load': {
                        'outputRows': 9,
                    },
                },
            },
        }

    def test_timing(self):
        with patch('airflow_metrics.airflow_metrics.patch_gcs_2_bq.Stats') as Stats:
            bq_upserted(self.ctx, self.self)
            assert Stats.gauge.call_args == (
                ('task.upserted.gcs_to_bq', 9),
                {'tags': {'dag': 'dag-id', 'task': 'task-id', 'operator': 'MockOperator'}},
            )


class TestBqDuration(TestCase):
    def setUp(self):
        self.self = Mock()
        self.self.__big_query_cursor__ = Mock()
        self.self.dag_id = 'dag-id'
        self.self.task_id = 'task-id'
        self.self.__class__.__name__ = 'MockOperator'
        self.ctx = {
            'success': True,
            'job': {
                'statistics': {
                    'creationTime': 0,
                    'startTime': 1,
                    'endTime': 3,
                },
            },
        }

    def test_timing(self):
        with patch('airflow_metrics.airflow_metrics.patch_gcs_2_bq.Stats') as Stats:
            bq_duration(self.ctx, self.self)
            assert Stats.timing.call_args_list == [
                (
                    ('task.delay.gcs_to_bq', 1),
                    {'tags': {'dag': 'dag-id', 'task': 'task-id', 'operator': 'MockOperator'}},
                ),
                (
                    ('task.duration.gcs_to_bq', 2),
                    {'tags': {'dag': 'dag-id', 'task': 'task-id', 'operator': 'MockOperator'}},
                ),
            ]
