[![Build Status](https://travis-ci.com/getsentry/airflow-metrics.svg?token=TJpWxpbKGxDuV8CPPRzL&branch=master)](https://travis-ci.com/getsentry/airflow-metrics)

# airflow-metrics

`airflow-metrics` is an Airflow plugin for automatically sending metrics from Airflow to Datadog.

**Tested For**: `apache-airflow>=1.10.2, <=1.10.3`

## Installation

```shell
pip install airflow-metrics
```

### Optional

If you want to the metrics from `BigQueryOperator` and `GoogleCloudStorageToBigQueryOperator`, then make sure the necessary dependencies are installed.

```shell
pip install apache-airflow[gcp_api]
```

## Setup

`airflow-metrics` will report all metrics to Datadog, so create an `airflow` connection with your Datadog api key.

```shell
airflow connections --add --conn_id datadog_default --conn_type HTTP --conn_extr '{"api_key": "<your api key>"}'
```

**Note**: If you skip this step, your `airflow` installation should still work but no metrics will be reported.

## Usage

That's it! `airflow-metrics` will now begin sending metrics from Airflow to Datadog automatically.

### Metrics

`airflow-metrics` will automatically begin reporting the following metrics

* `airflow.task.state` The total number of tasks in a state where the state is stored as a tag.
* `airflow.task.state.bq` The current number of big query tasks in a state where the state is stored as a tag.
* `airflow.dag.duration` The duration of a DAG in ms.
* `airflow.task.duration` The duration of a task in ms.
* `airflow.request.duration` The duration of a HTTP request in ms.
* `airflow.request.status.success` The current number of HTTP requests with successful status codes (<400)
* `airflow.request.status.failure` The current number of HTTP requests with unsuccessful status codes (>=400)
* `airflow.task.upserted.bq` The number of rows upserted by a BigQueryOperator.
* `airflow.task.delay.bq` The time taken for the big query job from a BigQueryOperator to start in ms.
* `airflow.task.duration.bq` The time taken for the big query job from a BigQueryOperator to finish in ms.
* `airflow.task.upserted.gcs_to_bq` The number of rows upserted by a GoogleCloudStorageToBigQueryOperator.
* `airflow.task.delay.gcs_to_bq` The time taken for the big query from a GoogleCloudStorageToBigQueryOperator to start in ms.
* `airflow.task.duration.gcs_to_bq` The time taken for the big query from a GoogleCloudStorageToBigQueryOperator to finish in ms.

## Configuration

By default, `airflow-metrics` will begin extracting metrics from Airflow as you run your DAGs and send them to Datadog. You can opt out of it entirely or opt out of a subset of the metrics by setting these configurations in your `airflow.cfg`

```
[airflow_metrics]

airflow_metrics_enabled = True
airflow_metrics_tasks_enabled = True
airflow_metrics_bq_enabled = True
airflow_metrics_gcs_to_bq_enabled = True
airflow_metrics_requests_enabled = True
airflow_metrics_thread_enabled = True`
```

## Limitations

`airflow-metrics` starts a thread to report some metrics, and is not supported when using sqlite as your database.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

### Getting Started

Set up your virtual environment for python3 however you like.

```shell
pip install -e .
airflow initdb
airflow connections --add --conn_id datadog_default --conn_type HTTP --conn_extr '{"api_key": ""}'
```

**Note**: The last step is necessary, otherwise the plugin will not initialize correctly and will not collect metrics. But you are free to add a dummy key for development purposes.

### Running Tests

```shell
pip install -r requirements-dev.txt
pytest
```
