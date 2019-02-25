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
    }
)

bash_operators = []
for operator in [1, 5, 10]:
    bash_operators.append(BashOperator(task_id="sleep_" + str(operator),
                                       bash_command="sleep " + str(operator),
                                       dag=dag))

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

print_date >> bash_operators >> dummy