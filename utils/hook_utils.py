from airflow.utils.log.logging_mixin import LoggingMixin
from functools import wraps


class HookManager(LoggingMixin):
    def __init__(self, cls, method_name):
        self.cls = cls
        self.method_name = method_name

        self.pre_hooks = []
        self.post_hooks = []

    def post_process(self):
        if self.pre_hooks:
            msg = 'post process only runs the post_hooks but {} pre_hook(s) found'
            self.log.warn(msg.format(len(self.pre_hooks)))

        # TODO: check that this is a method?
        method = getattr(self.cls, self.method_name)

        @wraps(method)
        def wrapped_method(*args, **kwargs):
            return_value = method(*args, **kwargs)

            context = {}
            for fn in self.post_hooks:
                return_value = fn(return_value, context, *args, **kwargs)

            return return_value

        setattr(self.cls, self.method_name, wrapped_method)

    def wrap_method(self, skip_on_fail=False):
        # TODO: check that this is a method?
        method = getattr(self.cls, self.method_name)

        @wraps(method)
        def wrapped_method(*args, **kwargs):
            context = {}
            for pre_hook in self.pre_hooks:
                pre_hook(context, *args, **kwargs)
            try:
                context['return'] = method(*args, **kwargs)
            except Exception as e:
                if not skip_on_fail:
                    for post_hook in self.post_hooks:
                        post_hook(context, *args, **kwargs)
                raise e
            else:
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
