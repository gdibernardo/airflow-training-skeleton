import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="hello_airflow",
    default_args={
        "owner": "godatadriven",
        "start_date": airflow.utils.dates.days_ago(3),
    },
)

# BashOperator(
#     task_id="print_exec_date", bash_command="echo {{ execution_date }}", dag=dag
# )

sleep_1 = BashOperator(
    task_id="sleep_1", bash_command="sleep 1", dag=dag
)

sleep_2 = BashOperator(
    task_id="sleep_5", bash_command="sleep 5", dag=dag
)

sleep_3 = BashOperator(
    task_id="sleep_10", bash_command="sleep 10", dag=dag
)

dummy = DummyOperator(
    task_id="the_end", dag=dag
)

def _print_exec_date(execution_date, **context):
    print(execution_date)

print_date = PythonOperator(
    task_id="print_execution_date",
    python_callable=_print_exec_date,
    provide_context=True,
    dag=dag)

print_date >> [sleep_1, sleep_2, sleep_3] >> dummy
