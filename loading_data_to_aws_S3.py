import boto3
from io import StringIO
from auth import ACCESS_KEY, SECRET_KEY
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from sqlalchemy import create_engine


def get_stackoverflow_data(ti) -> None:
    url = "https://api.stackexchange.com/2.3/tags?order=desc&sort=popular&site=stackoverflow"
    response = requests.get(url)
    ti.xcom_push(key='fetch_stackoverflow_data', value=response.json())

def transform_data(ti) -> None:
    response = ti.xcom_pull(key='fetch_stackoverflow_data', task_ids='get_stackoverflow_data')
    df = pd.dataframe(response['items'])
    df = df.drop(['has_synonyms','is_moderator_only','is_required'],axis=1)
    ti.xcom_push(key='fetch_stackoverflow_data', value=df.to_json(orient='split'))

def load_data_to_aws_s3(ti) -> None:
    response = ti.xcom_pull(key='fetch_stackoverflow_data', task_ids='get_stackoverflow_data')
    print(response)
    df=pd.read.to_json(orient='split')
    session = boto3.session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    #Creating S3 Ressource from session
    s3_res = session.ressource("S3")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    bucket = 'etl-bucket-s3'
    s3_object_name = 'df_raw1.csv'
    s3_res.Object(bucket, s3_object_name).put(Body=csv_buffer.getvalue())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    default_args=default_args,
    dag_id = 'fetch_stackoverflow_data',
    description = 'DAG to fetch stackoverflow data using API endpoint',
    start_date = datetime(2023,3,23),
    schedule_interval  = '@daily'
) as dag:
    task1 = PythonOperator(task_id = 'get_stackoverflow_data',
                           python_callable=get_stackoverflow_data
                           )
    task2 = PythonOperator(task_id = 'transform_stackoverflow_data',
                           python_callable=transform_data
                           )
    task3 = PythonOperator(task_id = 'load_data_to_aws_s3',
                           python_callable=get_stackoverflow_data
                           ) 
    task1 >> task2 >> task3
