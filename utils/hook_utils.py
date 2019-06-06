from airflow.utils.log.logging_mixin import LoggingMixin
from functools import wraps
from itertools import chain


class HookException(Exception):
    pass


class HookTransaction(LoggingMixin):
    def __init__(self,
                 pre_hooks,
                 post_hooks, skip_on_fail,
                 *args, **kwargs):
        self.pre_hooks = pre_hooks
        self.post_hooks = post_hooks
        self.skip_on_fail = skip_on_fail
        self.args = args
        self.kwargs = kwargs
        self.context = {}

    def __enter__(self):
        self.log.info('Running pre-hooks')
        for pre_hook in self.pre_hooks:
            pre_hook(self.context, *self.args, **self.kwargs)

    def __exit__(self, type, value, traceback):
        if self.skip_on_fail and traceback:
            # an error occured, skipping
            self.log.warning('An error has occured, skipping the post-hooks')
            return

        self.log.info('Running post-hooks')
        for post_hook in self.post_hooks:
            post_hook(self.context, *self.args, **self.kwargs)


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
            with self.transaction(skip_on_fail, *args, **kwargs):
                return method(*args, **kwargs)

        setattr(self.cls, self.method_name, wrapped_method)

    def transaction(self, skip_on_fail=False, *args, **kwargs):
        return HookTransaction(self.pre_hooks,
                               self.post_hooks,
                               skip_on_fail,
                               *args, **kwargs)

    def register_pre_hook(self, pre_hook):
        self.log.info('registering a pre-hook')
        self.pre_hooks.append(pre_hook)

    def register_post_hook(self, post_hook):
        self.log.info('registering a post-hook')
        self.post_hooks.append(post_hook)
