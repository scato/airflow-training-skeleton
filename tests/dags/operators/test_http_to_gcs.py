from datetime import datetime

import pendulum
import pytest
from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.http_hook import HttpHook

from dags.operators.http_to_gcs import HttpToGcsOperator
from unittest.mock import patch, ANY


@pytest.fixture
def test_operator():
    dag = DAG(dag_id='test', default_args={'start_date': datetime(2019, 1, 1)})

    with dag:
        http_to_gcs = HttpToGcsOperator(
            endpoint='/test?{{ ds }}/',
            bucket='my-bucket',
            filename='path/to/file',
            task_id='test')

    dag.clear()

    return http_to_gcs


@patch.object(GoogleCloudStorageHook, 'upload')
@patch.object(HttpHook, 'run')
def test_templates_endpoint(http_hook_run, gcs_hook_upload, test_operator):
    http_hook_run.return_value.content = b'{"foo": "bar"}'

    test_operator.run(
        start_date=pendulum.datetime(2019, 1, 1),
        end_date=pendulum.datetime(2019, 1, 1),
    )

    http_hook_run.assert_called_with('/test?2019-01-01/', None, None, None)


@patch.object(GoogleCloudStorageHook, 'upload')
@patch.object(HttpHook, 'run')
def test_writes_response_to_bucket(http_hook_run, gcs_hook_upload, test_operator):
    http_hook_run.return_value.content = b'{"foo": "bar"}'

    def assert_expected_content(*args):
        with open(args[2]) as fp:
            assert fp.read() == '{"foo": "bar"}'

    gcs_hook_upload.side_effect = assert_expected_content

    test_operator.run(
        start_date=pendulum.datetime(2019, 1, 1),
        end_date=pendulum.datetime(2019, 1, 1),
    )

    gcs_hook_upload.assert_called_with('my-bucket', 'path/to/file', ANY, 'application/json')
