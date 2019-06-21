import sys

from functools import wraps

from airflow import configuration as conf
from airflow.exceptions import AirflowConfigException
from airflow.models import BaseOperator
from airflow.utils.log.logging_mixin import LoggingMixin


LOG = LoggingMixin().log


def capture_exception(ex):
    try:
        from sentry_sdk import capture_exception as capture # pylint: disable=import-error
        capture(ex)
    except (ModuleNotFoundError, ImportError):
        LOG.warning(str(ex))


def enabled(metric='', default=True):
    if metric:
        metric = '{}_'.format(metric)
    metric = 'airflow_metrics_{}enabled'.format(metric)
    try:
        return conf.getboolean('airflow_metrics', metric)
    except AirflowConfigException:
        return default


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


def swallow_error(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as ex: # pylint: disable=broad-except
            capture_exception(ex)
            return None
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
        except Exception as ex: # pylint: disable=broad-except
            capture_exception(ex)

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
