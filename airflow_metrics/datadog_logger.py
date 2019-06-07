from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from atexit import register, unregister
from datadog import initialize, ThreadStats
from datetime import timedelta


class DatadogStatsLogger(BaseHook, LoggingMixin):
    def __init__(self, datadog_conn_id='datadog_default'):
        conn = self.get_connection(datadog_conn_id)
        self.api_key = conn.extra_dejson.get('api_key', None)
        self.app_key = conn.extra_dejson.get('app_key', None)
        self.source_type_name = conn.extra_dejson.get('source_type_name ', None)

        # If the host is populated, it will use that hostname instead
        # for all metric submissions
        self.host = conn.host

        if self.api_key is None:
            raise AirflowException('api_key must be specified in the '
                                   'Datadog connection details')

        self.log.info('Setting up api keys for Datadog')
        initialize(api_key=self.api_key, app_key=self.app_key)
        self.stats = ThreadStats()
        self.stats.start()

        register(self.stop)

    def incr(self, stat, count=1, rate=1, tags=None):
        self.log.info('datadog incr: {} {} {}'.format(stat, count, rate))
        self.stats.increment(stat, value=count, sample_rate=rate,
                             tags=self._format_tags(tags))

    def decr(self, stat, count=1, rate=1, tags=None):
        self.log.info('datadog decr: {} {} {}'.format(stat, count, rate))
        self.stats.decrement(stat, value=count, sample_rate=rate,
                             tags=self._format_tags(tags))

    def gauge(self, stat, value, rate=1, delta=False, tags=None):
        self.log.info('datadog gauge: {} {} {} {}'.format(stat, value, rate, delta))
        if delta:
            self.log.warning('Deltas are unsupported in Datadog')
        self.stats.gauge(stat, value, sample_rate=rate,
                         tags=self._format_tags(tags))

    def timing(self, stat, delta, rate=1, tags=None):
        self.log.info('datadog timing: {} {}'.format(stat, delta))
        if isinstance(delta, timedelta):
            delta = delta.total_seconds() * 1000.
        self.stats.timing(stat, delta, sample_rate=rate,
                          tags=self._format_tags(tags))

    def _format_tags(self, tags):
        if not tags:
            return None
        return ['{}:{}'.format(k, v) for k, v in tags.items()]

    def stop(self):
        unregister(self.stop)
        self.stats.stop()
