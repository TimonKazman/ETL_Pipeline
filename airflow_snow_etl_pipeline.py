import os
from datetime import datetime, timedelta
import requests
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
import logging
import json

default_args = {
    'owner': 'Timon',
    'schedule_interval': '0 */2 * * *'  
}
#triggered every two minutes

def extract_data_from_serviceNow():
    
    logging.info('Start ETL Job')

    url = 'https://dev108966.service-now.com/api/now/table/metric_instance?sysparm_display_value=true&sysparm_exclude_reference_link=true&sysparm_limit=1000'
    
    user = 'usr'
    pwd = 'pwd'

    headers = {
        "Content-Type":"application/json",
        "Accept":"application/json", 
        "accept-encoding": "gzip, deflate, br"
        }

    response = requests.get(url, auth=(user, pwd), headers=headers )

    if response.status_code != 200: 
        print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:',response.json())
        exit()

    data = response.json()
    df = pd.json_normalize(data, record_path  = ['result'])
    return df

def transform_datafram(df):

    logging.info('Start Transform DF')

    df["start"] = pd.to_datetime(df.start)
    return df

def load_dataframe_to_folder(df):
        
    #data_path = f"/opt/airflow/dags/data{datetime.now()}.xlsx"
    data_path2 = "C:\\Users\\timon\\airflow\\dags\\snow.xlsx"
    df.to_excel(data_path2)

    logging.info('Done ETL')

dag = DAG('Snow_etl_pipeline', description='ETL_pipeline',
          default_args=default_args,
          start_date=datetime(2022, 8, 18), catchup=False)


pull_data = PythonOperator(
    task_id='extract', 
    python_callable=extract_data_from_serviceNow, 
    dag=dag)    

transform_data = PythonOperator(
    task_id='transform',
    python_callable=transform_datafram, 
    provide_context = True, 
    dag=dag)   

load_data = PythonOperator(
    task_id='load',
    python_callable=load_dataframe_to_folder,
    provide_context= True,
    dag=dag)    
    
pull_data >> transform_data >> load_data