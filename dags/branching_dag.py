import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

dag = DAG(
    dag_id="branching_dag",
    default_args={
        "owner": "Gabriele Di Bernardo",
        "start_date": airflow.utils.dates.days_ago(14),
    }
)

weekday_person_to_email = {
    0: "Bob",  # Monday
    1: "Joe",  # Tuesday
    2: "Alice",  # Wednesday
    3: "Joe",  # Thursday
    4: "Alice",  # Friday
    5: "Alice",  # Saturday
    6: "Alice",  # Sunday
}

job_map = {
    "Bob": "email_bob",
    "Joe": "email_joe",
    "Alice": "email_alice"
}


def _pick_a_branch(execution_date, **context):
    weekday = execution_date.weekday()
    return job_map[weekday_person_to_email[weekday]]


def _print_exec_date(execution_date, **context):
    print(execution_date)


print_date = PythonOperator(
    task_id="print_branching_date",
    python_callable=_print_exec_date,
    provide_context=True,
    dag=dag)

branching = BranchPythonOperator(task_id='branching_operator',
                                 python_callable=_pick_a_branch,
                                 provide_context=True,
                                 dag=dag)

final_task = DummyOperator(task_id='final_task',
                           dag=dag,
                           trigger_rule=TriggerRule.ONE_SUCCESS)

for key, value in job_map.items():
    branching >> DummyOperator(task_id='' + value, dag=dag) >> final_task

print_date >> branching
