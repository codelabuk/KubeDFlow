import json
from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import requests
import pandas as pd

def fetch_with_data(**kwargs):
    url = 'https://raw.githubusercontent.com/codelabuk/KubeDFlow/refs/heads/master/uploads/new-output.csv'
    response = requests.get(url)

    if response.status_code == 200:
        df = pd.read_csv(url, header=None, names=['Category', 'Price', 'Quantity'])
        json_data = df.to_json(orient='records')
        kwargs['ti'].xcom_push(key='data', value=json_data)
    else:
        raise Exception("Failed to get data, HTTP status code: {}".format(response.status_code))


def preview_data(**kwargs):
    output_data = kwargs['ti'].xcom_pull(key='data', task_ids='get_data')
    print(output_data)
    if output_data:
        output_data = json.loads(output_data)
    else:
        raise ValueError('No data received from XCom')

    df = pd.DataFrame(output_data)
    df['Total'] = df['Price'] * df['Quantity']
    df = (df.groupby('Category',False)
          .agg({'Quantity': 'sum', 'Total': 'sum'}))
    df = df.sort_values(by='Total', ascending=False)
    print(df[['Category', 'Total']].head(20))


default_args = {
    'owner': 'codelabuk',
    'start_date': datetime(2026, 4, 3),
    'catchup': False
}


dag = DAG(
    'fetch_with_data',
    default_args = default_args,
    schedule= timedelta(days=1),
)

get_data_from_url = PythonOperator(
    task_id='get_data',
    python_callable=fetch_with_data,
    dag=dag
)

preview_data_from_url = PythonOperator(
    task_id='preview_data',
    python_callable=preview_data,
    dag=dag
)

get_data_from_url >> preview_data_from_url