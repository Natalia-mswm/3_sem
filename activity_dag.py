from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import sys

# Добавляем путь к директории с transform_script.py
sys.path.append('/opt/airflow/data')
from transform_script import transfrom

# Определяем параметры DAG
default_args = {
    'owner': 'max',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Создаем DAG
dag = DAG(
    'activity_flags_Frolov_Maksim',
    default_args=default_args,
    description='Calculate customer activity flags',
    schedule_interval='0 0 5 * *',  # Запуск 5-го числа каждого месяца
    catchup=False
)

def extract():
    """Извлекаем данные из CSV файла"""
    try:
        df = pd.read_csv('/opt/airflow/data/profit_table.csv')
        df['date'] = pd.to_datetime(df['date'])
        print(f"Данные успешно загружены. Количество строк: {len(df)}")
        return df
    except Exception as e:
        print(f"Ошибка при чтении файла: {str(e)}")
        raise

def transform_data(df):
    """Трансформируем данные"""
    try:
        current_date = datetime.now().strftime('%Y-%m-01')
        transformed_df = transfrom(df, current_date)
        print(f"Данные успешно трансформированы. Количество строк: {len(transformed_df)}")
        return transformed_df
    except Exception as e:
        print(f"Ошибка при трансформации данных: {str(e)}")
        raise

def load_data(df):
    """Сохраняем результаты в CSV"""
    output_path = '/opt/airflow/data/flags_activity.csv'
    try:
        # Добавляем дату расчета
        current_date = datetime.now().strftime('%Y-%m-01')
        df['calculation_date'] = current_date

        if os.path.exists(output_path):
            # Читаем существующие данные
            existing_df = pd.read_csv(output_path)

            # Удаляем старые данные за текущую дату, если они есть
            if 'calculation_date' in existing_df.columns:
                existing_df = existing_df[existing_df['calculation_date'] != current_date]

            # Добавляем новые данные
            final_df = pd.concat([existing_df, df], ignore_index=True)
        else:
            final_df = df

        # Сохраняем результат
        final_df.to_csv(output_path, index=False)
        print(f"Данные за {current_date} успешно добавлены в {output_path}")
        print(f"Всего записей в файле: {len(final_df)}")

    except Exception as e:
        print(f"Ошибка при сохранении данных: {str(e)}")
        raise

def etl():
    """Основная ETL функция"""
    try:
        print("Начало ETL процесса")
        data = extract()
        transformed_data = transform_data(data)
        load_data(transformed_data)
        print("ETL процесс успешно завершен")
    except Exception as e:
        print(f"Ошибка в ETL процессе: {str(e)}")
        raise

# Создаем задачу
etl_task = PythonOperator(
    task_id='calculate_activity_flags_Frolov_Maksim',
    python_callable=etl,
    dag=dag
)
