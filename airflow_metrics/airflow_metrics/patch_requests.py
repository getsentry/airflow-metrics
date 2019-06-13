from datetime import datetime
from functools import wraps
from urllib.parse import urlparse

from airflow.settings import Stats
from airflow.utils.log.logging_mixin import LoggingMixin
from requests import PreparedRequest
from requests import Session

from airflow_metrics.utils.fn_utils import get_calling_operator
from airflow_metrics.utils.fn_utils import once
from airflow_metrics.utils.hook_utils import HookManager


LOG = LoggingMixin().log

BLACKLIST = {
    'api.datadoghq.com',
}


def attach_request_meta(ctx, *args, **kwargs):
    if len(args) >= 2 and isinstance(args[1], PreparedRequest):
        request = args[1]
        url = request.url
    else:
        LOG.warning('No url found for request')
        return
    ctx['url'] = url

    domain = urlparse(url).netloc
    if domain in BLACKLIST:
        LOG.warning('Found blacklisted domain: {}'.format(url))
        return
    ctx['domain'] = domain

    operator = get_calling_operator()
    if not operator:
        LOG.warning('Request not made by an operator: {}'.format(url))
        return
    ctx['operator'] = operator


def whitelisted(func):
    @wraps(func)
    def wrapped(ctx, *args, **kwargs):
        if ctx.get('url') and ctx.get('domain') and ctx.get('operator'):
            return func(ctx, *args, **kwargs)
        return None
    return wrapped


@whitelisted
def start_time(ctx, *args, **kwargs):
    ctx['start_time'] = datetime.now()


@whitelisted
def stop_time(ctx, *args, **kwargs):
    start = ctx['start_time']
    stop = datetime.now()
    duration = (stop - start).total_seconds() * 1000

    tags = {
        'dag': ctx['operator'].dag_id,
        'task': ctx['operator'].task_id,
        'operator': ctx['operator'].__class__.__name__,
        'domain': ctx['domain'],
    }
    Stats.timing('request.duration', duration, tags=tags)


@HookManager.success_only
@whitelisted
def http_status(ctx, *args, **kwargs):
    response = ctx['return']
    status = response.status_code

    tags = {
        'dag': ctx['operator'].dag_id,
        'task': ctx['operator'].task_id,
        'operator': ctx['operator'].__class__.__name__,
        'domain': ctx['domain'],
        'status': status
    }
    if status < 400:
        Stats.incr('request.status.success', tags=tags)
    else:
        Stats.incr('request.status.failure', tags=tags)


@once
def patch_requests():
    session_request_manager = HookManager(Session, 'send')
    session_request_manager.register_pre_hook(attach_request_meta)
    session_request_manager.register_pre_hook(start_time)
    session_request_manager.register_post_hook(stop_time)
    session_request_manager.register_post_hook(http_status)
    session_request_manager.wrap_method()
