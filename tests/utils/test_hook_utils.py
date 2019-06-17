from unittest import TestCase

from pytest import raises

from airflow_metrics.utils.hook_utils import HookManager
from tests.utility import mockfn


class TestHookManager(TestCase):
    def test_patch_method(self):
        class TestClass():
            def test_method(self):
                pass

        test_method_manager = HookManager(TestClass, 'test_method')
        test_method_manager.wrap_method()

        fake_method_manager = HookManager(TestClass, 'fake_method')
        with raises(AttributeError):
            fake_method_manager.wrap_method()

    def test_swallows_errors(self):
        class TestClass():
            def test_method(self):
                return True

        def pre_hook(*args, **kwargs):
            raise Exception()

        def post_hook(*args, **kwargs):
            raise Exception()

        test_method_manager = HookManager(TestClass, 'test_method')
        test_method_manager.register_pre_hook(pre_hook)
        test_method_manager.register_post_hook(post_hook)
        test_method_manager.wrap_method()

        test_obj = TestClass()
        assert test_obj.test_method()

    def test_call_order(self):
        call_order = []

        class TestClass():
            def test_method(self):
                call_order.append('test method')

        def pre_hook_1(*args, **kwargs):
            call_order.append('pre-hook 1')

        def pre_hook_2(*args, **kwargs):
            call_order.append('pre-hook 2')

        def post_hook_1(*args, **kwargs):
            call_order.append('post-hook 1')

        def post_hook_2(*args, **kwargs):
            call_order.append('post-hook 2')

        test_method_manager = HookManager(TestClass, 'test_method')
        test_method_manager.register_pre_hook(pre_hook_1)
        test_method_manager.register_pre_hook(pre_hook_2)
        test_method_manager.register_post_hook(post_hook_1)
        test_method_manager.register_post_hook(post_hook_2)
        test_method_manager.wrap_method()

        test_obj = TestClass()
        assert not call_order

        test_obj.test_method()
        assert call_order == [
            'pre-hook 1',
            'pre-hook 2',
            'test method',
            'post-hook 1',
            'post-hook 2'
        ]

    def test_carries_context(self):
        class TestClass():
            def test_method(self):
                pass

        @mockfn
        def pre_hook_1(ctx, *args, **kwargs):
            ctx['pre-hook 1'] = True

        @mockfn
        def pre_hook_2(ctx, *args, **kwargs):
            assert ctx['pre-hook 1']
            ctx['pre-hook 2'] = True

        @mockfn
        def post_hook_1(ctx, *args, **kwargs):
            assert ctx['pre-hook 1']
            assert ctx['pre-hook 2']
            ctx['post-hook 1'] = True

        @mockfn
        def post_hook_2(ctx, *args, **kwargs):
            assert ctx['pre-hook 1']
            assert ctx['pre-hook 2']
            assert ctx['post-hook 1']

        test_method_manager = HookManager(TestClass, 'test_method')
        test_method_manager.register_pre_hook(pre_hook_1)
        test_method_manager.register_pre_hook(pre_hook_2)
        test_method_manager.register_post_hook(post_hook_1)
        test_method_manager.register_post_hook(post_hook_2)
        test_method_manager.wrap_method()

        test_obj = TestClass()
        assert not pre_hook_1.called
        assert not pre_hook_2.called
        assert not post_hook_1.called
        assert not post_hook_2.called
        test_obj.test_method()
        assert pre_hook_1.called
        assert pre_hook_2.called
        assert post_hook_1.called
        assert post_hook_2.called

    def test_success_status_true(self):
        class TestClass():
            def test_method(self):
                pass

        @mockfn
        def pre_hook(ctx, *args, **kwargs):
            assert 'success' not in ctx

        @mockfn
        def post_hook(ctx, *args, **kwargs):
            assert ctx['success']

        test_method_manager = HookManager(TestClass, 'test_method')
        test_method_manager.register_pre_hook(pre_hook)
        test_method_manager.register_post_hook(post_hook)
        test_method_manager.wrap_method()

        test_obj = TestClass()
        assert not pre_hook.called
        assert not post_hook.called
        test_obj.test_method()
        assert pre_hook.called
        assert post_hook.called

    def test_success_status_false(self):
        class TestClass():
            def test_method(self):
                raise Exception()

        @mockfn
        def pre_hook(ctx, *args, **kwargs):
            assert not ctx['success']

        @mockfn
        def post_hook(ctx, *args, **kwargs):
            assert not ctx['success']

        test_method_manager = HookManager(TestClass, 'test_method')
        test_method_manager.register_pre_hook(pre_hook)
        test_method_manager.register_post_hook(post_hook)
        test_method_manager.wrap_method()

        test_obj = TestClass()
        with raises(Exception):
            test_obj.test_method()

    def test_pass_on_return_value(self):
        class TestClass():
            def test_method(self):
                return 'return value'

        @mockfn
        def post_hook(ctx, *args, **kwargs):
            assert ctx['return'] == 'return value'

        test_method_manager = HookManager(TestClass, 'test_method')
        test_method_manager.register_post_hook(post_hook)
        test_method_manager.wrap_method()

        test_obj = TestClass()
        assert not post_hook.called
        test_obj.test_method()
        assert post_hook.called

    def test_modify_return_value(self):
        class TestClass():
            def test_method(self):
                return 'return value'

        def post_hook(ctx, *args, **kwargs):
            ctx['return'] = 'modified value'

        test_method_manager = HookManager(TestClass, 'test_method')
        test_method_manager.register_post_hook(post_hook)
        test_method_manager.wrap_method()

        test_obj = TestClass()
        assert test_obj.test_method() == 'modified value'

    def test_success_only_true(self):
        class TestClass():
            def test_method(self):
                pass

        @mockfn
        def post_hook_mock(*args, **kwargs):
            pass
        post_hook = HookManager.success_only(post_hook_mock)

        test_method_manager = HookManager(TestClass, 'test_method')
        test_method_manager.register_post_hook(post_hook)
        test_method_manager.wrap_method()

        test_obj = TestClass()
        assert not post_hook_mock.called
        test_obj.test_method()
        assert post_hook_mock.called

    def test_success_only_false(self):
        class TestClass():
            def test_method(self):
                raise Exception()

        @mockfn
        def post_hook_mock(*args, **kwargs):
            pass
        post_hook = HookManager.success_only(post_hook_mock)

        test_method_manager = HookManager(TestClass, 'test_method')
        test_method_manager.register_post_hook(post_hook)
        test_method_manager.wrap_method()

        test_obj = TestClass()
        assert not post_hook_mock.called
        with raises(Exception):
            test_obj.test_method()
        assert not post_hook_mock.called
