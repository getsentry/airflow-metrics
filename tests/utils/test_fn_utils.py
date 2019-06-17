from unittest import TestCase

from airflow.models import BaseOperator

from airflow_metrics.utils.fn_utils import once
from airflow_metrics.utils.fn_utils import get_calling_operator
from airflow_metrics.utils.fn_utils import get_local_vars
from airflow_metrics.utils.fn_utils import swallow_error
from tests.utility import mockfn


class TestOnce(TestCase):
    def test_once(self):
        @mockfn
        def fn_mock():
            pass
        func = once(fn_mock)
        assert fn_mock.call_count == 0
        func()
        assert fn_mock.call_count == 1
        func()
        assert fn_mock.call_count == 1


class TestSwallowError(TestCase):
    def test_swallow_error(self):
        @swallow_error
        def error():
            raise Exception()

        error()


class TestGetLocalVars(TestCase):
    def test_get_local_vars(self):
        # the mockfn decorator addes additional frames onto the
        # call stack thus the values are larger than expected

        @mockfn
        def inner():
            local_vars = get_local_vars(6)
            assert local_vars['a'] == 1
            assert local_vars['b'] == 2
            local_vars = get_local_vars(3)
            assert local_vars['c'] == 3
            assert local_vars['d'] == 4

        @mockfn
        def middle(c):
            d = 4
            local_vars = get_local_vars(3)
            assert local_vars['a'] == 1
            assert local_vars['b'] == 2
            inner()
            del c
            del d

        @mockfn
        def outer(a):
            b = 2
            local_vars = get_local_vars(0)
            assert local_vars['a'] == 1
            assert local_vars['b'] == 2
            middle(3)
            del a
            del b

        assert not outer.called
        assert not middle.called
        assert not inner.called
        outer(1)
        assert outer.called
        assert middle.called
        assert inner.called


class TestGetCallingOperator(TestCase):
    def test_called_by_operator(self):
        @mockfn
        def test_fn(self):
            assert get_calling_operator() is self

        class MyOperator(BaseOperator):
            def execute(self, context):
                assert get_calling_operator() is self
                test_fn(self)

        operator = MyOperator(task_id='im-a-test')
        assert not test_fn.called
        operator.execute(None)
        assert test_fn.called

    def test_called_by_out_of_range_operator(self):
        @mockfn
        def test_fn():
            assert get_calling_operator(2) is None

        class MyOperator(BaseOperator):
            def execute(self, context):
                assert get_calling_operator(2) is self
                test_fn()

        operator = MyOperator(task_id='im-a-test')
        assert not test_fn.called
        operator.execute(None)
        assert test_fn.called

    def test_not_called_by_operator(self):
        assert get_calling_operator() is None

        @mockfn
        def test_fn():
            assert get_calling_operator() is None

        assert not test_fn.called
        test_fn()
        assert test_fn.called
