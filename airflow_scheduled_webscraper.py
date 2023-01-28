import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
import logging

default_args = {
    'owner': 'Timon',
    'schedule_interval': '0 */2 * * *'  
}

def extract_data_stocks():
    logging.info('Start ETL Job')

        #get columns
    url = "https://markets.businessinsider.com/index/components/s&p_500?p=1"

    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    col = soup.find_all("th", class_="table__th")

    pattern = r"<(.*?)\>"
    columns = []
    for row in col:
        columns.append(re.sub(r"\n", " ", re.sub(pattern,"", str(row)).strip()))

    #get rows
    rows = []
    for i in range(1,100):
        url = "https://markets.businessinsider.com/index/components/s&p_500?p=" + str(i)
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        data = soup.find_all("td", class_="table__td")

        if not data:
            break
        else: 
            for row in data:
                rows.append(re.sub(r"\n", " ", re.sub(pattern,"", str(row)).strip()))
            
        data = np.array(rows)
        data = data.reshape(len(rows)//8, 8)
        df = pd.DataFrame(data, columns=columns)
    
    return df

def transform_dataframe(df):

    logging.info("Start Transform DF")

    df[["Latest Price", "Previous Close"]] = df["Latest Price Previous Close"].str.split(" ", 1, expand = True)
    df[["Latest Low_High", "Previous Low_High"]] = df['Low High'].str.split(" ", 1, expand = True)
    df[["Latest +/- %", "Previous +/- %"]] = df['+/- %'].str.split(" ", 1, expand = True)
    df[["Latest 3 Mo. +/- %", "Previous 3 Mo. +/- %"]] = df['3 Mo. +/- %'].str.split(" ", 1, expand = True)
    df[["Latest 6 Mo. +/- %", "Previous 6 Mo. +/- %"]] = df['6 Mo. +/- %'].str.split(" ", 1, expand = True)
    df[["Latest 1 Year +/- %", "Previous 1 Year +/- %"]] = df['1 Year +/- %'].str.split(" ", 1, expand = True)

    df.drop(['Latest Price Previous Close', 'Low High', '+/- %','3 Mo. +/- %', '6 Mo. +/- %', '1 Year +/- %'], inplace=True, axis=1)
    
    return df

def load_dataframe_to_folder(df):
    logging.info("Start Loading DF")
    e = datetime.datetime.now()
    e = str(e).split(" ")[0]
    filepath = "C:\\Users\\timon\\airflow\\dags\\stocks.xlsx"
    with pd.ExcelWriter(filepath, engine="openpyxl", mode="a", if_sheet_exists="overlay") as writer:
        df.to_excel(writer, sheet_name=e,index=False)
    
    logging.info("Done ETL")

pull_data = PythonOperator(
    task_id = "extract",
    python_callable=extract_data_stocks,
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform',
    python_callable=transform_dataframe, 
    provide_context = True, 
    dag=dag) 

load_data = PythonOperator(
    task_id='load',
    python_callable=load_dataframe_to_folder,
    provide_context= True,
    dag=dag)  

pull_data >> transform_data >> load_data

