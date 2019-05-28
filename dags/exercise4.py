from airflow import DAG
import airflow.utils.dates
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

args = {
    'start_date': airflow.utils.dates.days_ago(14),
}

dag = DAG(
    dag_id='exercise4',
    default_args=args,
)

with dag:
    psql_to_gcs = PostgresToGoogleCloudStorageOperator(
        sql="""
            SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'
            """,
        bucket='europe-west1-training-airfl-097953ee-bucket',
        filename='data/{{ ds }}/properties_{}.json',
        postgres_conn_id='postgres_gdd',
        task_id='extract_land_registry_price_paid_uk',
    )
