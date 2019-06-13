from collections import defaultdict

from airflow.utils.log.logging_mixin import LoggingMixin
from sqlalchemy import event
from sqlalchemy import inspect


class EventManager(LoggingMixin):
    def __init__(self, cls, event_name):
        super().__init__()
        self.cls = cls
        self.event_name = event_name
        self.callbacks = defaultdict(list)

        def listener(mapper, connection, target):
            del mapper
            del connection

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
        event.listens_for(self.cls, self.event_name)(listener)


    def register_callback(self, state, callback):
        self.log.info('registering a callback')
        self.callbacks[state].append(callback)
