from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import requests
import pymysql

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='incremental_update_weather_split',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Split version of incremental update with actual and forecast weather data"
)

def get_last_actual_date(**context):
    conn = BaseHook.get_connection("mysql_weather")
    db = pymysql.connect(
        host=conn.host,
        user=conn.login,
        password=conn.password,
        database=conn.schema,
        port=conn.port
    )
    cursor = db.cursor()
    cursor.execute("SELECT MAX(date) FROM weather_data WHERE data_type = 'A' AND state = 'NY'")
    last_date = cursor.fetchone()[0]
    if not last_date:
        raise Exception("No actual data found in the database.")
    db.close()
    context['ti'].xcom_push(key='last_actual_date', value=last_date.strftime('%Y-%m-%d'))

def fetch_and_prepare_weather_data(**context):
    last_date = context['ti'].xcom_pull(task_ids='get_last_actual_date', key='last_actual_date')
    today = datetime.today().strftime('%Y-%m-%d')

    combined_data = []

    hist_url = "https://archive-api.open-meteo.com/v1/archive"
    hist_params = {
        "latitude": 40.7128,
        "longitude": -74.0060,
        "start_date": last_date,
        "end_date": today,
        "daily": "temperature_2m_min,temperature_2m_max,relative_humidity_2m_max,precipitation_sum,rain_sum,snowfall_sum,wind_speed_10m_max,weather_code",
        "timezone": "America/New_York",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch"
    }

    hist_resp = requests.get(hist_url, params=hist_params)
    hist_data = hist_resp.json()

    if "daily" not in hist_data:
        raise Exception("Historical data missing from API response")

    for i in range(len(hist_data["daily"]["time"])):
        combined_data.append((
            hist_data["daily"]["time"][i],
            "NY", "A",
            hist_data["daily"]["temperature_2m_min"][i],
            hist_data["daily"]["temperature_2m_max"][i],
            hist_data["daily"]["relative_humidity_2m_max"][i],
            hist_data["daily"]["precipitation_sum"][i],
            hist_data["daily"]["rain_sum"][i],
            hist_data["daily"]["snowfall_sum"][i],
            hist_data["daily"]["wind_speed_10m_max"][i],
            hist_data["daily"]["weather_code"][i]
        ))

    forecast_url = "https://api.open-meteo.com/v1/forecast"
    forecast_params = {
        "latitude": 40.7128,
        "longitude": -74.0060,
        "daily": "temperature_2m_min,temperature_2m_max,relative_humidity_2m_max,precipitation_sum,rain_sum,snowfall_sum,wind_speed_10m_max,weather_code",
        "forecast_days": 16,
        "timezone": "America/New_York",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch"
    }

    forecast_resp = requests.get(forecast_url, params=forecast_params)
    forecast_data = forecast_resp.json()

    if "daily" not in forecast_data:
        raise Exception("Forecast data missing from API response")

    for i in range(len(forecast_data["daily"]["time"])):
        combined_data.append((
            forecast_data["daily"]["time"][i],
            "NY", "P",
            forecast_data["daily"]["temperature_2m_min"][i],
            forecast_data["daily"]["temperature_2m_max"][i],
            forecast_data["daily"]["relative_humidity_2m_max"][i],
            forecast_data["daily"]["precipitation_sum"][i],
            forecast_data["daily"]["rain_sum"][i],
            forecast_data["daily"]["snowfall_sum"][i],
            forecast_data["daily"]["wind_speed_10m_max"][i],
            forecast_data["daily"]["weather_code"][i]
        ))

    context['ti'].xcom_push(key='combined_weather_data', value=combined_data)

def insert_incremental_weather_data(**context):
    data = context['ti'].xcom_pull(task_ids='fetch_and_prepare_weather_data', key='combined_weather_data')
    if not data:
        raise Exception("No data fetched to insert")

    conn = BaseHook.get_connection("mysql_weather")
    db = pymysql.connect(
        host=conn.host,
        user=conn.login,
        password=conn.password,
        database=conn.schema,
        port=conn.port,
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = db.cursor()

    for row in data:
        cursor.execute("""
            REPLACE INTO weather_data (
                date, state, data_type, temperature_2m_min, temperature_2m_max,
                relative_humidity_2m_max, precipitation_sum, rain_sum,
                snowfall_sum, wind_speed_10m_max, weather_code
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, row)

    db.commit()
    cursor.close()
    db.close()


t1 = PythonOperator(
    task_id='get_last_actual_date',
    python_callable=get_last_actual_date,
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id='fetch_and_prepare_weather_data',
    python_callable=fetch_and_prepare_weather_data,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='insert_incremental_weather_data',
    python_callable=insert_incremental_weather_data,
    provide_context=True,
    dag=dag,
)

t1 >> t2 >> t3
