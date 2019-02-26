import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

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

dummy_end = DummyOperator(
    task_id="the_end", dag=dag
)

pgsl_to_gcs >> dummy_end