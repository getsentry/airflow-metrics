from airflow.utils.log.logging_mixin import LoggingMixin
from functools import wraps


class HookManager(LoggingMixin):
    def __init__(self, cls, method_name):
        self.cls = cls
        self.method_name = method_name

        self.pre_hooks = []
        self.post_hooks = []

    def wrap_method(self):
        # TODO: check that this is a method?
        method = getattr(self.cls, self.method_name)

        @wraps(method)
        def wrapped_method(*args, **kwargs):
            context = {'success': False}
            for pre_hook in self.pre_hooks:
                pre_hook(context, *args, **kwargs)
            try:
                context['return'] = method(*args, **kwargs)
                context['success'] = True
            finally:
                for post_hook in self.post_hooks:
                    post_hook(context, *args, **kwargs)

            return context['return']

        setattr(self.cls, self.method_name, wrapped_method)

    def register_pre_hook(self, pre_hook):
        self.log.info('registering a pre-hook: {}'.format(pre_hook.__name__))
        self.pre_hooks.append(pre_hook)

    def register_post_hook(self, post_hook):
        self.log.info('registering a post-hook: {}'.format(post_hook.__name__))
        self.post_hooks.append(post_hook)

    @classmethod
    def success_only(cls, fn):
        @wraps(fn)
        def wrapped(ctx, *args, **kwargs):
            if not ctx['success']:
                return
            return fn(ctx, *args, **kwargs)
        return wrapped
