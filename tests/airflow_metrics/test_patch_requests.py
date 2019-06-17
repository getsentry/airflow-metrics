from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock
from unittest.mock import patch
from urllib.parse import urlparse

from airflow.models import BaseOperator
from freezegun import freeze_time
from requests import PreparedRequest

from airflow_metrics.airflow_metrics.patch_requests import attach_request_meta
from airflow_metrics.airflow_metrics.patch_requests import http_status
from airflow_metrics.airflow_metrics.patch_requests import start_time
from airflow_metrics.airflow_metrics.patch_requests import stop_time


URL = 'https://httpbin.org/get'
DOMAIN = urlparse(URL).netloc

def whitelisted_context():

    operator = Mock()
    operator.dag_id = 'dag-id'
    operator.task_id = 'task-id'
    operator.__class__.__name__ = 'MockOperator'

    return {
        'url': URL,
        'domain': DOMAIN,
        'operator': operator,
    }


class TestAttachRequestMeta(TestCase):
    def setUp(self):
        self.request = PreparedRequest()
        self.request.prepare(url=URL)

    def test_no_url(self):
        ctx = {}
        args = []
        kwargs = {}
        attach_request_meta(ctx, *args, **kwargs)
        assert 'url' not in ctx

    def test_not_prepared_request(self):
        ctx = {}
        args = [1, 2]
        kwargs = {}
        attach_request_meta(ctx, *args, **kwargs)
        assert 'url' not in ctx

    def test_blacklisted(self):
        with patch('airflow_metrics.airflow_metrics.patch_requests.BLACKLIST', {DOMAIN}):
            ctx = {}
            args = [1, self.request]
            kwargs = {}
            attach_request_meta(ctx, *args, **kwargs)
            assert 'domain' not in ctx

    def test_not_called_by_operator(self):
        ctx = {}
        args = [1, self.request]
        kwargs = {}
        attach_request_meta(ctx, *args, **kwargs)
        assert 'operator' not in ctx

    def test_correct(self):
        this = self

        class MockOperator(BaseOperator):
            def execute(self, context):
                ctx = {}
                args = [1, this.request]
                kwargs = {}
                attach_request_meta(ctx, *args, **kwargs)
                assert ctx['url'] == URL
                assert ctx['domain'] == DOMAIN
                assert ctx['operator'] == operator

        operator = MockOperator(task_id='task-id')
        operator.execute(None)


class TestStartTime(TestCase):
    @freeze_time('2019-01-01')
    def test_inserts_start_time(self):
        ctx = whitelisted_context()
        start_time(ctx)
        assert ctx['start_time'] == datetime(2019, 1, 1)


class TestStopTime(TestCase):
    @freeze_time('2019-01-01 00:05:03')
    def test_timing(self):
        with patch('airflow_metrics.airflow_metrics.patch_requests.Stats') as Stats:
            ctx = whitelisted_context()
            ctx['start_time'] = datetime(2019, 1, 1)
            stop_time(ctx)
            assert Stats.timing.call_args == (
                ('request.duration', 303000.0),
                {
                    'tags': {
                        'dag': 'dag-id',
                        'task': 'task-id',
                        'operator': 'MockOperator',
                        'domain': DOMAIN,
                    }
                },
            )


class TestHttpStatus(TestCase):
    def test_succcessful_http(self):
        with patch('airflow_metrics.airflow_metrics.patch_requests.Stats') as Stats:
            ctx = whitelisted_context()
            response = Mock()
            response.status_code = 200
            ctx['return'] = response
            http_status(ctx)
            assert Stats.incr.call_args == (
                ('request.status.success',),
                {
                    'tags': {
                        'dag': 'dag-id',
                        'task': 'task-id',
                        'operator': 'MockOperator',
                        'domain': DOMAIN,
                        'status': 200,
                    }
                },
            )

    def test_failed_http(self):
        with patch('airflow_metrics.airflow_metrics.patch_requests.Stats') as Stats:
            ctx = whitelisted_context()
            response = Mock()
            response.status_code = 400
            ctx['return'] = response
            http_status(ctx)
            assert Stats.incr.call_args == (
                ('request.status.failure',),
                {
                    'tags': {
                        'dag': 'dag-id',
                        'task': 'task-id',
                        'operator': 'MockOperator',
                        'domain': DOMAIN,
                        'status': 400,
                    }
                },
            )
