from airflow.settings import Stats
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow_metrics.utils.fn_utils import get_local_vars, get_calling_operator
from airflow_metrics.utils.fn_utils import once
from airflow_metrics.utils.hook_utils import HookManager
from datetime import datetime
from functools import wraps
from requests import Session
from urllib.parse import urlparse


log = LoggingMixin().log

blacklist = {
    'api.datadoghq.com',
}


def attach_request_meta(ctx, *args, **kwargs):
    url = kwargs.get('url')
    if not url:
        if len(args) >= 3:
            url = args[2]
        else:
            log.warning('No url found for request')
            return
    ctx['url'] = url

    domain = urlparse(url).netloc
    if domain in blacklist:
        log.warning('Found blacklisted domain: {}'.format(url))
        return
    ctx['domain'] = domain

    operator = get_calling_operator()
    if not operator:
        log.warning('Request not made by an operator: {}'.format(url))
        return
    ctx['operator'] = operator


def whitelisted(fn):
    @wraps(fn)
    def wrapped(ctx, *args, **kwargs):
        if ctx.get('url') and ctx.get('domain') and ctx.get('operator'):
            return fn(ctx, *args, **kwargs)
    return wrapped


@whitelisted
def start_time(ctx, *args, **kwargs):
    ctx['start_time'] = datetime.now()


@whitelisted
def stop_time(ctx, *args, **kwargs):
    start_time = ctx['start_time']
    stop_time = datetime.now()
    duration = (stop_time - start_time).total_seconds() * 1000

    tags = {
        'dag': ctx['operator'].dag_id,
        'task': ctx['operator'].task_id,
        'operator': ctx['operator'].__class__.__name__,
        'domain': ctx['domain'],
    }
    Stats.timing('request.duration', duration, tags=tags)


@whitelisted
def http_status(ctx, *args, **kwargs):
    # in the event it threw and error, skip this step
    if not ctx.get('return'):
        return

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
    session_request_manager = HookManager(Session, 'request')
    session_request_manager.register_pre_hook(attach_request_meta)
    session_request_manager.register_pre_hook(start_time)
    session_request_manager.register_post_hook(stop_time)
    session_request_manager.register_post_hook(http_status)
    session_request_manager.wrap_method()
