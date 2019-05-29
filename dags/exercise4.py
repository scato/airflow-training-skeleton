import os.path
import sys

from airflow import DAG
import airflow.utils.dates
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.dataproc_operator \
    import DataprocClusterCreateOperator, DataProcPySparkOperator, DataprocClusterDeleteOperator
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

sys.path.append(os.path.dirname(os.path.dirname(__file__)))  # NOQA

from dags.operators.http_to_gcs import HttpToGoogleCloudStorageOperator

args = {
    'start_date': airflow.utils.dates.days_ago(14),
}

dag = DAG(
    dag_id='exercise4',
    default_args=args,
)

project_id = 'airflowbolcom-may2829-ba473316'

with dag:
    psql_to_gcs = PostgresToGoogleCloudStorageOperator(
        sql="""
            SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = %(ds)s
            """,
        parameters={'ds': '{{ ds }}'},
        bucket='europe-west1-training-airfl-097953ee-bucket',
        filename='data/properties/ds={{ ds }}/properties_{}.json',
        postgres_conn_id='postgres_gdd',
        task_id='extract_land_registry_price_paid_uk',
    )

    http_to_gcs = HttpToGoogleCloudStorageOperator(
        endpoint='/airflow-training-transform-valutas?date={{ ds }}&to=EUR',
        bucket='europe-west1-training-airfl-097953ee-bucket',
        filename='data/currencies/ds={{ ds }}/properties_0.json',
        http_conn_id='http_gdd',
        task_id='http_to_gcs',
    )

    dataproc_create_cluster = DataprocClusterCreateOperator(
        cluster_name='analyse-pricing-{{ ds }}',
        project_id=project_id,
        zone='europe-west4-a',
        num_workers=2,
        task_id='dataproc_create_cluster',
    )

    dataproc_pyspark = DataProcPySparkOperator(
        main='gs://europe-west1-training-airfl-097953ee-bucket/build_statistics.py',
        arguments=[
            'gs://europe-west1-training-airfl-097953ee-bucket/data/properties/ds={{ ds }}/',
            'gs://europe-west1-training-airfl-097953ee-bucket/data/currencies/ds={{ ds }}/',
            'gs://europe-west1-training-airfl-097953ee-bucket/data/statistics/ds={{ ds }}/',
        ],
        cluster_name='analyse-pricing-{{ ds }}',
        project_id=project_id,
        zone='europe-west4-a',
        task_id='dataproc_pyspark',
    )

    dataproc_delete_cluster = DataprocClusterDeleteOperator(
        cluster_name='analyse-pricing-{{ ds }}',
        project_id=project_id,
        zone='europe-west4-a',
        task_id='dataproc_delete_cluster',
    )

    dataflow_python = DataFlowPythonOperator(
        py_file='dataflow_job.py',
        options={
            'input': 'gs://europe-west1-training-airfl-097953ee-bucket/data/properties/ds={{ ds }}/',
            'table': 'properties',
            'dataset': 'airflowbolcom-may2829-ba473316:dwh',
        },
        dataflow_default_options={
            'project': project_id,
            'region': 'europe-west1',
            'staging_location': 's://europe-west1-training-airfl-097953ee-bucket/dataflow-staging',
            'temp_location': 's://europe-west1-training-airfl-097953ee-bucket/dataflow-temp',
        },
        task_id='dataflow_python',
    )

    gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        bucket='europe-west1-training-airfl-097953ee-bucket',
        source_objects=[
            '/data/statistics/ds={{ ds }}/part-*'
        ],
        destination_project_dataset_table='airflowbolcom-may2829-ba473316:dwh.statistics',
        source_format='PARQUET',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        google_cloud_storage_conn_id='google_cloud_storage_default',
        bigquery_conn_id='bigquery_default',
        task_id='gcs_to_bq',
    )

    psql_to_gcs >> dataflow_python

    psql_to_gcs >> dataproc_pyspark
    http_to_gcs >> dataproc_pyspark
    dataproc_create_cluster >> dataproc_pyspark

    dataproc_pyspark >> gcs_to_bq
    dataproc_pyspark >> dataproc_delete_cluster
