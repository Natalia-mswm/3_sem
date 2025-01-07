from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from transform_script import transform

# Путь к файлу с данными
profit_table_path = "./data/profit_table.csv"
output_path = "./data/flags_activity.csv"

def extract():
    """Функция для извлечения данных из CSV"""
    if os.path.exists(profit_table_path):
        return pd.read_csv(profit_table_path)
    else:
        raise FileNotFoundError(f"Файл {profit_table_path} не найден!")

def transform_data(profit_table, date):
    """Функция для трансформации данных с использованием кода дата-сайентистов"""
    return transform(profit_table, date)

def load_data(df):
    """Функция для загрузки данных в CSV"""
    if os.path.exists(output_path):
        df.to_csv(output_path, mode='a', header=False, index=False)
    else:
        df.to_csv(output_path, mode='w', header=True, index=False)

# Определение DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'client_activity_dag',
    default_args=default_args,
    description='ETL процесс для расчёта витрины активности клиентов',
    schedule_interval='@monthly',  # Запуск каждый месяц
)

# Операции в DAG
def transform_task_callable(*args, **kwargs):
    """Передача аргументов через XCom и вызов функции трансформации"""
    profit_table = kwargs['ti'].xcom_pull(task_ids='extract')
    date = kwargs['date']
    return transform_data(profit_table, date)

# Задачи DAG
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_task_callable,
    op_args=['{{ task_instance.xcom_pull(task_ids="extract") }}'],
    op_kwargs={'date': '2024-03-01'},
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    op_args=['{{ task_instance.xcom_pull(task_ids="transform") }}'],
    dag=dag,
)

# Определяем порядок выполнения
extract_task >> transform_task >> load_task

