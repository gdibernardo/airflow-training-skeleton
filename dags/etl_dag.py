import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from airflow_training.operators.http_to_gcs import HttpToGcsOperator

dag = DAG(
    dag_id='etl',
    schedule_interval='@daily',
    default_args={
        'owner': 'Gabriele',
        'start_date': airflow.utils.dates.days_ago(14),
    }
)

pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id= "pgsql_to_gcs",
    postgres_conn_id= "pg_connection",
    sql= """SELECT * 
         FROM public.land_registry_price_paid_uk
         WHERE transfer_date >= date '{{ execution_date }}'
          AND transfer_date < date '{{ next_execution_date }}'
          """,
    bucket="gabriele-bucket",
    filename="pg_export/{{ ds }}/properties_{}",
    dag=dag,
)

http_to_gcs_ops = []
for currency in {'EUR', 'USD'}:
    http_to_gcs = HttpToGcsOperator(task_id="http_to_gcs_{}".format(currency),
                                http_conn_id="http_connection",
                                endpoint="/convert-currency?date={{ ds }}&from=GBP&to=" + currency,
                                bucket="gabriele-bucket",
                                filename="currency/{{ ds }}/" + currency + "/change",
                                dag=dag)
    http_to_gcs_ops.append(http_to_gcs)

dummy_end = DummyOperator(
    task_id="the_end", dag=dag
)

pgsl_to_gcs >> http_to_gcs_ops >> dummy_end