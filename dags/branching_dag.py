import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

dag = DAG(
    dag_id="branching_dag",
    default_args={
        "owner": "Gabriele Di Bernardo",
        "start_date": airflow.utils.dates.days_ago(14),
    }
)

branching_options = ['email_joe', 'email_bob', 'email_alice']

def _pick_a_branch(execution_date, **context):
    print(execution_date.weekday())
    return branching_options[execution_date.weekday()]

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

final_task = DummyOperator(task_id='final_task', dag=dag)

for option in branching_options:
    print_date >> branching >> DummyOperator(task_id=option, dag=dag) >> final_task
