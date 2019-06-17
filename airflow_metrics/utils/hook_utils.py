from functools import wraps

from airflow.utils.log.logging_mixin import LoggingMixin

from airflow_metrics.utils.fn_utils import swallow_error


class HookManager(LoggingMixin):
    def __init__(self, cls, method_name):
        super().__init__()
        self.cls = cls
        self.method_name = method_name

        self.pre_hooks = []
        self.post_hooks = []

    def wrap_method(self):
        method = getattr(self.cls, self.method_name)

        @wraps(method)
        def wrapped_method(*args, **kwargs):
            hook_context = {}

            self.run_pre_hooks(hook_context, *args, **kwargs)

            try:
                if 'return' in hook_context:
                    # in the event that the method fails, ensure 'return' is not a key
                    del hook_context['return']
                hook_context['return'] = method(*args, **kwargs)
            except Exception as ex:
                hook_context['success'] = False
                raise ex
            else:
                hook_context['success'] = True
            finally:
                self.run_post_hooks(hook_context, *args, **kwargs)

            return hook_context['return']

        setattr(self.cls, self.method_name, wrapped_method)

    @swallow_error
    def run_pre_hooks(self, hook_context, *args, **kwargs):
        for pre_hook in self.pre_hooks:
            pre_hook(hook_context, *args, **kwargs)

    @swallow_error
    def run_post_hooks(self, hook_context, *args, **kwargs):
        for post_hook in self.post_hooks:
            post_hook(hook_context, *args, **kwargs)

    def register_pre_hook(self, pre_hook):
        self.log.info('registering a pre-hook: {}'.format(pre_hook.__name__))
        self.pre_hooks.append(pre_hook)

    def register_post_hook(self, post_hook):
        self.log.info('registering a post-hook: {}'.format(post_hook.__name__))
        self.post_hooks.append(post_hook)

    @classmethod
    def success_only(cls, func):
        @wraps(func)
        def wrapped(ctx, *args, **kwargs):
            if 'success' in ctx and not ctx['success']:
                return None
            return func(ctx, *args, **kwargs)
        return wrapped
