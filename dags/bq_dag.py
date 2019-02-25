import airflow
from airflow import DAG

from bq.bigquery_get_data import BigQueryGetDataOperator

dag = DAG(
    dag_id='godatafest',
    schedule_interval='@daily',
    default_args={
        'owner': 'GoDataDriven',
        'start_date': airflow.utils.dates.days_ago(2)
    }
)

bq_fetch_data = BigQueryGetDataOperator(
    task_id='bq_fetch_data',
    sql="""
        SELECT
            author.name, count(commit) as commits
        FROM (FLATTEN([bigquery-public-data.github_repos.commits], repo_name))
        WHERE
            repo_name like "apache/airflow"
        GROUP BY
            author.name
        ORDER BY
            commits DESC
        LIMIT
            5
        """,
    dag=dag,
    xcom_push=True,
    provide_context=True
)

from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def send_to_slack_func(**context):
    pass


send_to_slack = PythonOperator(
    task_id='send_to_slack',
    python_callable=send_to_slack_func,
    provide_context=True,
    dag=dag,
)

bq_fetch_data >> send_to_slack