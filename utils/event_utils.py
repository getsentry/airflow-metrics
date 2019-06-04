from airflow.utils.log.logging_mixin import LoggingMixin
from collections import defaultdict
from sqlalchemy import event, inspect


class EventManager(LoggingMixin):
    def __init__(self, cls, e):
        self.cls = cls
        self.e = e
        self.callbacks = defaultdict(list)

        @event.listens_for(self.cls, self.e)
        def listener(mapper, conenction, target):
            state = inspect(target)
            for attr, callbacks in self.callbacks.items():
                 history = state.get_history(attr, True)
                 if not history.has_changes():
                     continue

                 for callback in callbacks:
                     old = None
                     if history.deleted and history.deleted[0]:
                         old = history.deleted[0]  # not too clear on why this is a list

                     new = None
                     if history.added and history.added[0]:
                         new = history.added[0]  # not too clear on why this is a list

                     callback(target=target, new=new, old=old)


    def register_callback(self, state, callback):
        self.log.info('registering a callback')
        self.callbacks[state].append(callback)
