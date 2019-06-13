import sys

from functools import wraps

from airflow.models import BaseOperator


def once(func):
    context = {
        'ran': False,
    }

    @wraps(func)
    def wrapped(*args, **kwargs):
        if context['ran']: # turn the second call and onwards into noop
            return None
        context['ran'] = True

        return func(*args, **kwargs)

    return wrapped


def get_local_vars(frame_number=0):
    try:
        frame = sys._getframe(frame_number + 1) # pylint: disable=protected-access
        local_vars = frame.f_locals
        return local_vars
    finally:
        try:
            del frame
            del local_vars
        except: # pylint: disable=bare-except
            pass


def get_calling_operator(max_frames=25):
    for i in range(max_frames):
        try:
            local_vars = get_local_vars(i)
        except ValueError:
            return None

        self = local_vars.get('self', None)

        if self is None:
            continue

        if isinstance(self, BaseOperator):
            return self
    return None
