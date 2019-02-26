import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from airflow_training.operators.http_to_gcs import HttpToGcsOperator

from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

PROJECT_ID = 'airflowbolcom-58aea67718d62a47'

dag = DAG(
    dag_id='etl',
    schedule_interval='@daily',
    default_args={
        'owner': 'Gabriele',
        'start_date': airflow.utils.dates.days_ago(14)
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
    filename="pg_export/{{ ds }}/properties_{}.json",
    dag=dag)

http_to_gcs_ops = []
for currency in {'EUR', 'USD'}:
    http_to_gcs = HttpToGcsOperator(task_id="http_to_gcs_{}".format(currency),
                                http_conn_id="http_connection",
                                endpoint="/convert-currency?date={{ ds }}&from=GBP&to=" + currency,
                                bucket="gabriele-bucket",
                                filename="currency/{{ ds }}/" + currency + ".json",
                                dag=dag)
    http_to_gcs_ops.append(http_to_gcs)

dummy_end = DummyOperator(
    task_id="the_end", dag=dag)

from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator)

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id=PROJECT_ID,
    num_workers=2,
    zone="europe-west4-a",
    dag=dag)

compute_aggregates = DataProcPySparkOperator(
    task_id='compute_aggregates',
    main='gs://europe-west1-training-airfl-67643e8c-bucket/build_statistics.py',
    cluster_name='analyse-pricing-{{ ds }}',
    arguments=[
    "gs://gabriele-bucket/pg_export/{{ ds }}/*.json",
    "gs://gabriele-bucket/currency/{{ ds }}/*.json",
    "gs://gabriele-bucket/average_prices/{{ ds }}/"
    ],
    dag=dag)


from airflow.utils.trigger_rule import TriggerRule
dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id=PROJECT_ID,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag)

from airflow_training.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

write_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id="write_to_bq",
    bucket="gabriele-bucket",
    source_objects=["average_prices/{{ ds }}/*.parquet"],
    destination_project_dataset_table=PROJECT_ID + ":prices.land_registry_price${{ ds_nodash }}",
    source_format="PARQUET",
    write_disposition="WRITE_TRUNCATE",
    dag=dag)


land_registry_prices_to_bigquery = DataFlowPythonOperator(
    task_id="land_registry_prices_to_bigquery",
    dataflow_default_options={
        'region': "europe-west1",
        'input': 'gs://gabriele-bucket/pg_export/{{ ds }}/*.json',
        'table': 'registry',
        'dataset': 'prices',
        'project': PROJECT_ID,
        'staging_location':'gs://gabriele-bucket/stg_location/',
        'temp_location': 'gs://gabriele-bucket/temp_location/',
        'job_name': '{{ task_instance_key_str }}',
    },
    py_file="gs://europe-west1-training-airfl-67643e8c-bucket/dataflow_job.py",
    dag=dag)

pgsl_to_gcs >> dataproc_create_cluster

http_to_gcs_ops >> dataproc_create_cluster

http_to_gcs_ops >> land_registry_prices_to_bigquery >> dummy_end

dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster >> dummy_end

compute_aggregates >> write_to_bq >> dummy_end
