import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator


def _print_weekday(execution_date, **context):
    print(execution_date.format('%A'))


def _branching(execution_date, **context):
    weekday_key = int(execution_date.format('%w'))
    person = weekday_person_to_email[weekday_key].lower()

    return f'email_{person}'


args = {
    'owner': 'your name',
    'start_date': airflow.utils.dates.days_ago(14),
}

dag = DAG(
    dag_id='exercise3',
    default_args=args,
)

weekday_person_to_email = {
    0: 'Bob',    # Monday
    1: 'Joe',    # Tuesday
    2: 'Alice',  # Wednesday
    3: 'Joe',    # Tuesday
    4: 'Alice',  # Wednesday
    5: 'Alice',  # Wednesday
    6: 'Alice',  # Wednesday
}

with dag:
    print_weekday = PythonOperator(
        python_callable=_print_weekday,
        provide_context=True,
        task_id='print_weekday',
    )

    branching = BranchPythonOperator(
        python_callable=_branching,
        provide_context=True,
        task_id='branching',
    )

    email_people = [
        BashOperator(
            bash_command=f'echo Email {name}',
            task_id=f'email_{name}',
        )
        for name in weekday_person_to_email
    ]

    final_task = DummyOperator(
        task_id='final_task',
    )

    print_weekday >> branching >> email_people >> final_task
