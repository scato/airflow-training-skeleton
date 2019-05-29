import os.path
import sys

from airflow import DAG
import airflow.utils.dates
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
        num_workers=2,
        zone='europe-west4-a',
        task_id='dataproc_create_cluster',
    )

    dataproc_pyspark = DataProcPySparkOperator(
        cluster_name='analyse-pricing-{{ ds }}',
        project_id=project_id,
        pyfiles=['other/build_statistics.py'],
        arguments=[
            'gs://europe-west1-training-airfl-097953ee-bucket/data/properties/',
            'gs://europe-west1-training-airfl-097953ee-bucket/data/currencies/',
            'gs://europe-west1-training-airfl-097953ee-bucket/data/statistics/',
        ],
        zone='europe-west4-a',
        task_id='dataproc_pyspark',
    )

    dataproc_delete_cluster = DataprocClusterDeleteOperator(
        cluster_name='analyse-pricing-{{ ds }}',
        project_id=project_id,
        zone='europe-west4-a',
        task_id='dataproc_delete_cluster',
    )

    psql_to_gcs >> dataproc_pyspark
    http_to_gcs >> dataproc_pyspark
    dataproc_create_cluster >> dataproc_pyspark

    dataproc_pyspark >> dataproc_delete_cluster
