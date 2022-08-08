from datetime import datetime, timedelta
import work_with_data

from airflow import DAG
from airflow.operators.python import PythonOperator


with DAG(
        'load_to_snowflake',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': timedelta(days=8),
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=8),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['example'],
) as dag:
    creating_streams = PythonOperator(
        task_id="creating_streams",
        python_callable=work_with_data._creating_streams
    )

    load_to_snowflake = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=work_with_data._load_to_snowflake
    )

    from_raw_to_stage = PythonOperator(
        task_id="from_raw_to_stage",
        python_callable=work_with_data._from_raw_to_stage
    )

    from_stage_to_master = PythonOperator(
        task_id="from_stage_to_master",
        python_callable=work_with_data._from_stage_to_master
    )

    creating_streams >> load_to_snowflake >> from_raw_to_stage >> from_stage_to_master
