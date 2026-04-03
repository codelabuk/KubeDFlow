from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    'owner': 'codelabuk',
    'start_date': datetime(2026, 4, 3),
    'catchup': False
}


dag = DAG(
    'hello_world',
    default_args = default_args,
    schedule= timedelta(days=1),
)

t1 = BashOperator(
    task_id='hello world',
    bash_command='echo "hello world"',
    dag=dag
)

t2= BashOperator(
    task_id='hello codelabuk',
    bash_command='echo "hello codelabuk"',
    dag=dag
)

t1 >> t2