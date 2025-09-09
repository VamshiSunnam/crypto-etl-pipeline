from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'crypto_etl_pipeline',
    default_args=default_args,
    description='A simple crypto ETL pipeline',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

extract = BashOperator(
    task_id='extract',
    bash_command='python /app/etl.py extract --save_path=raw.json',
    dag=dag,
)

transform = BashOperator(
    task_id='transform',
    bash_command='python /app/etl.py transform --input_path=raw.json --output_path=crypto_top_50.csv',
    dag=dag,
)

load = BashOperator(
    task_id='load',
    bash_command='python /app/etl.py load --csv_path=crypto_top_50.csv',
    dag=dag,
)

validate = BashOperator(
    task_id='validate',
    bash_command='python /app/etl.py validate --csv_path=crypto_top_50.csv',
    dag=dag,
)

extract >> transform >> validate >> load
