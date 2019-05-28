import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def _print_execution_date(execution_date, **context):
    print(execution_date)


args = {'owner': 'your_name', 'start_date': airflow.utils.dates.days_ago(14)}

dag = DAG(
    dag_id='exercise2',
    default_args=args,
)

with dag:
    print_execution_date = PythonOperator(
        python_callable=_print_execution_date,
        task_id='print_execution_date',
        provide_context=True,
    )

    the_end = DummyOperator(
        task_id='the_end',
    )

    for seconds in [5, 10, 20]:
        wait = BashOperator(
            bash_command=f'sleep {seconds}',
            task_id=f'wait_{seconds}',
        )

        print_execution_date >> wait
        wait >> the_end
