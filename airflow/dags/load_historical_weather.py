from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta, date
import pymysql
import requests
import logging

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2023, 1, 1),
}

def fetch_weather_data(**context):
    log = context['ti'].log
    start_date = "2015-01-01"
    end_date = date.today().strftime("%Y-%m-%d")
    
    log.info(f"Fetching data from {start_date} to {end_date}")
    
    response = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": 40.7128,
            "longitude": -74.0060,
            "start_date": start_date,
            "end_date": end_date,
            "daily": ",".join([
                "temperature_2m_min", "temperature_2m_max",
                "relative_humidity_2m_max", "precipitation_sum",
                "rain_sum", "snowfall_sum",
                "wind_speed_10m_max", "weather_code"
            ]),
            "timezone": "America/New_York",
            "temperature_unit": "fahrenheit",
            "wind_speed_unit": "mph",
            "precipitation_unit": "inch"
        },
        timeout=120
    )
    
    response.raise_for_status()
    data = response.json()["daily"]
    context['ti'].xcom_push(key='weather_raw', value=data)
    log.info("Weather data fetched successfully.")

def transform_weather_data(**context):
    raw_data = context['ti'].xcom_pull(task_ids='fetch_weather_data', key='weather_raw')
    dates = raw_data["time"]
    transformed = []

    for i, d in enumerate(dates):
        transformed.append((
            d, 'NY', 'A',
            raw_data.get("temperature_2m_min", [None])[i],
            raw_data.get("temperature_2m_max", [None])[i],
            raw_data.get("relative_humidity_2m_max", [None])[i],
            raw_data.get("precipitation_sum", [None])[i],
            raw_data.get("rain_sum", [None])[i],
            raw_data.get("snowfall_sum", [None])[i],
            raw_data.get("wind_speed_10m_max", [None])[i],
            raw_data.get("weather_code", [None])[i]
        ))

    context['ti'].xcom_push(key='weather_transformed', value=transformed)
    context['ti'].log.info(f"Transformed {len(transformed)} records.")

def insert_weather_data(**context):
    conn = BaseHook.get_connection("mysql_weather")
    db = pymysql.connect(
        host=conn.host,
        user=conn.login,
        password=conn.password,
        database=conn.schema,
        port=conn.port or 3306,
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = db.cursor()

    data = context['ti'].xcom_pull(task_ids='transform_weather_data', key='weather_transformed')
    if not data:
        context['ti'].log.info("No data to insert.")
        return

    for row in data:
        cursor.execute('''
            INSERT INTO weather_data (
                date, state, data_type,
                temperature_2m_min, temperature_2m_max,
                relative_humidity_2m_max, precipitation_sum,
                rain_sum, snowfall_sum, wind_speed_10m_max, weather_code
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', row)

    db.commit()
    cursor.close()
    db.close()
    context['ti'].log.info(f"Inserted {len(data)} records successfully.")

with DAG(
    dag_id="load_historical_weather_split_tasks",
    default_args=default_args,
    description="Split historical weather DAG with separate tasks",
    schedule_interval=None,
    catchup=False,
    tags=["weather", "historical"]
) as dag:

    t1 = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data
    )

    t2 = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather_data
    )

    t3 = PythonOperator(
        task_id='insert_weather_data',
        python_callable=insert_weather_data
    )

    t1 >> t2 >> t3
