from airflow import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

dag = DAG(
    dag_id='exercise 4',
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
