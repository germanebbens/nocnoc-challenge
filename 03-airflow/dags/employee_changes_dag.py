from datetime import timedelta
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.param import Param
from airflow.utils.dates import days_ago

from utils.alerts import task_failure_callback

CSV_DIR = "/output/reports/employee_changes"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def calculate_date_range(**context):
    execution_date = context['logical_date']
    
    params = context['params']
    if 'start_date' in params and 'end_date' in params and params['start_date'] and params['end_date']:
        start_date = params['start_date']
        end_date = params['end_date']
    else:
        first_day = (execution_date.replace(day=1) - timedelta(days=1)).replace(day=1)
        last_day = execution_date.replace(day=1) - timedelta(days=1)
        start_date = first_day.strftime('%Y-%m-%d')
        end_date = last_day.strftime('%Y-%m-%d')
    
    context['ti'].xcom_push(key='start_date', value=start_date)
    context['ti'].xcom_push(key='end_date', value=end_date)
    
    return {
        'start_date': start_date,
        'end_date': end_date
    }

def call_process_api(**context):
    start_date = context['ti'].xcom_pull(key='start_date', task_ids='calculate_date_range')
    end_date = context['ti'].xcom_pull(key='end_date', task_ids='calculate_date_range')
    
    url = f"http://spark_processing:5000/process?start_date={start_date}&end_date={end_date}"
    
    response = requests.get(url)
    
    if response.status_code == 202:
        return f"Processing initiated for period: {start_date} to {end_date}"
    else:
        raise Exception(f"API returned unexpected status code: {response.status_code}, Response: {response.text}")

def upload_to_s3(**context):
    """
    Simulates uploading the generated CSV report to an S3 bucket
    """  
    return f"File uploaded to S3 (simulated): {context['params']['s3_bucket_dir']}"

with DAG(
    'employee_changes_monthly_report',
    default_args=default_args,
    description='Generates a monthly report of changes in the employee base',
    schedule_interval='0 3 1 * *', # running 3 AM on first day of each month
    start_date=days_ago(1),
    catchup=False,
    on_failure_callback=task_failure_callback,
    params={
        'start_date': Param('', type='string'),
        'end_date': Param('', type='string'),
        's3_bucket_dir': Param('/output/s3', type='string')
    },
) as dag:
    
    # step 1: calculate the date range for the report
    calculate_dates = PythonOperator(
        task_id='calculate_date_range',
        python_callable=calculate_date_range,
        provide_context=True,
    )
    
    # step 2: clean any previous report files
    clean_previous_reports = BashOperator(
        task_id='clean_previous_reports',
        bash_command=f'chmod -R 777 {CSV_DIR} && rm -f {CSV_DIR}/*.csv || true',
    )
    
    # sep 3: call the Spark process with an API, to generate the report
    call_api = PythonOperator(
        task_id='call_api',
        python_callable=call_process_api,
        provide_context=True,
    )
    
    # step 4: wait the CSV file
    wait_for_csv = FileSensor(
        task_id='wait_for_csv',
        filepath=f'{CSV_DIR}/*.csv',
        poke_interval=30,
        timeout=600,
        mode='poke',
    )
    
    # step 5: load report to S3
    upload_to_s3_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        provide_context=True,
    )
    
    notify_success = DummyOperator(
        task_id='notify_success',
    )
    
    calculate_dates >> clean_previous_reports >> call_api >> wait_for_csv >> upload_to_s3_task >> notify_success