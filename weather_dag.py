from airflow import DAG
import pandas as pd
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import boto3
import requests
import json
import logging

s3_client = boto3.client('s3')

target_bucket_name = 'exchange-rate-zone'

api_url = 'https://api.exchangeratesapi.io/v1/latest'
api_key = '7e7fde675468e4b4b2436dbcd470507a'

logger = logging.getLogger(__name__)


def get_latest_exchange_rates(**kwargs):
    response = requests.get(f"{api_url}?access_key={api_key}")
    if response.status_code == 200:
        data = response.json()
        now = datetime.now()
        date_now_string = now.strftime('%d%m%Y%H%M%S')
        file_str = f'exchange_rate_{date_now_string}.csv'
        df = pd.DataFrame(data['rates'].items(), columns=['Currency', 'Rate'])
        df.to_csv(file_str, index=False)
        logger.info(f"Data fetched successfully and saved to {file_str}.")
        return file_str, data  # Return the filename and data for the next task
    else:
        raise Exception(f"Error fetching data: {response.status_code} - {response.text}")


def transformation(task_instance):
    
    xcom_value = task_instance.xcom_pull(task_ids="tsk_extract_exchange_rate_data")
    logger.info(f"XCom value: {xcom_value}")

    
    if isinstance(xcom_value, tuple) and len(xcom_value) == 2:
        file_str, data = xcom_value
    else:
        raise ValueError("Unexpected XCom value format. Expected a tuple of two items.")

   
    df = pd.read_csv(file_str)
    df.rename(columns={col: 'Currency' for col in df.columns if col == ' ' or 'Unnamed' in col}, inplace=True)
    df.drop(columns=[col for col in ['timestamp', 'date', 'time'] if col in df.columns], inplace=True)
    current_date = datetime.now().strftime('%m/%d/%Y')
    current_time = datetime.now().strftime('%H:%M:%S')
    df.insert(1, 'date', value=[current_date] * len(df))
    df.insert(2, 'time', value=[current_time] * len(df))
    csv_data = df.to_csv(index=False)
    object_key = f'{file_str}'
    s3_client.put_object(Bucket=target_bucket_name, Key=object_key, Body=csv_data)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 6),
    'retries': 3,
    'retry_delay': timedelta(seconds=15)
}

with DAG('exchange_rate_analysis',
         default_args=default_args,
         schedule_interval = '@daily',
         catchup=False) as dag:

         extract_data = PythonOperator(
         task_id = 'tsk_extract_exchange_rate_data',
         python_callable=get_latest_exchange_rates,
         provide_context = True,
         )

         transform_data = PythonOperator(
         task_id = 'tsk_transform_exchange_rate_data',
         python_callable=transformation,
         provide_context= True,
         )

         extract_data >> transform_data