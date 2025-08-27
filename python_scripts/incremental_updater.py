import requests
import pymysql
from dotenv import load_dotenv
import os
from datetime import datetime, date, timedelta

load_dotenv()

def get_db_connection():
    return pymysql.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME'),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def fetch_actual_data(start_date, end_date):
    variable_groups = [
        "temperature_2m_max,temperature_2m_min,relative_humidity_2m_max",
        "precipitation_sum,rain_sum,snowfall_sum",
        "wind_speed_10m_max",
        "weather_code"
    ]
    combined_data = {'daily': {}}

    for group in variable_groups:
        try:
            response = requests.get(
                "https://archive-api.open-meteo.com/v1/archive",
                params={
                    "latitude": 40.7128,
                    "longitude": -74.0060,
                    "start_date": start_date,
                    "end_date": end_date,
                    "daily": group,
                    "timezone": "America/New_York",
                    "temperature_unit": "fahrenheit",
                    "wind_speed_unit": "mph",
                    "precipitation_unit": "inch",
                },
                timeout=60
            )
            response.raise_for_status()
            data = response.json()

            if not combined_data['daily'].get('time'):
                combined_data['daily']['time'] = data['daily']['time']

            for key in data['daily']:
                if key != 'time':
                    combined_data['daily'][key] = data['daily'][key]

        except Exception as e:
            print(f"Error fetching group {group} from {start_date} to {end_date}: {e}")
            return None

    return combined_data if combined_data['daily'].get('time') else None

def fetch_forecast_data():
    try:
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": 40.7128,
                "longitude": -74.0060,
                "daily": "temperature_2m_min,temperature_2m_max,relative_humidity_2m_max,precipitation_sum,wind_speed_10m_max,weather_code",
                "forecast_days": 16,
                "timezone": "America/New_York",
                "temperature_unit": "fahrenheit",
                "wind_speed_unit": "mph",
                "precipitation_unit": "inch",
            },
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Forecast API error: {e}")
        return None

def update_actuals():
    print("Updating actuals:")
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT MAX(date) as last_actual FROM weather_data WHERE data_type = 'A'")
            row = cursor.fetchone()
            last_date = row['last_actual'] or date(2015, 1, 1)
            start_date = last_date + timedelta(days=1)
            end_date = date.today() - timedelta(days=1)

            if start_date > end_date:
                print("No new actual data to fetch.")
                return

            data = fetch_actual_data(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            if not data:
                print("No actual data fetched.")
                return

            dates = data['daily']['time']
            for i, date_str in enumerate(dates):
                cursor.execute("SELECT * FROM weather_data WHERE date = %s AND state = 'NY'", (date_str,))
                existing = cursor.fetchone()

                if existing:
                    if existing['data_type'] == 'P':
                        cursor.execute("DELETE FROM weather_data WHERE date = %s AND state = 'NY'", (date_str,))
                    elif existing['data_type'] == 'A':
                        fields = [
                            'temperature_2m_min', 'temperature_2m_max',
                            'relative_humidity_2m_max', 'precipitation_sum',
                            'rain_sum', 'snowfall_sum', 'wind_speed_10m_max', 'weather_code'
                        ]
                        if all(existing.get(field) is None for field in fields):
                            cursor.execute("DELETE FROM weather_data WHERE date = %s AND state = 'NY'", (date_str,))
                        else:
                            continue

                cursor.execute('''
                    INSERT INTO weather_data (date, state, data_type,
                        temperature_2m_min, temperature_2m_max,
                        relative_humidity_2m_max, precipitation_sum,
                        rain_sum, snowfall_sum, wind_speed_10m_max,
                        weather_code)
                    VALUES (%s, %s, 'A', %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    date_str, 'NY',
                    data['daily'].get('temperature_2m_min', [None]*len(dates))[i],
                    data['daily'].get('temperature_2m_max', [None]*len(dates))[i],
                    data['daily'].get('relative_humidity_2m_max', [None]*len(dates))[i],
                    data['daily'].get('precipitation_sum', [None]*len(dates))[i],
                    data['daily'].get('rain_sum', [None]*len(dates))[i],
                    data['daily'].get('snowfall_sum', [None]*len(dates))[i],
                    data['daily'].get('wind_speed_10m_max', [None]*len(dates))[i],
                    data['daily'].get('weather_code', [None]*len(dates))[i],
                ))
        connection.commit()
        print(f"Inserted actuals for {len(dates)} days.")
    finally:
        connection.close()

def update_forecast():
    print("Updating forecast:")
    forecast = fetch_forecast_data()
    if not forecast:
        print("No forecast data fetched.")
        return

    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            dates = forecast['daily']['time']
            metrics = forecast['daily']

            for i, date_str in enumerate(dates):
                cursor.execute("SELECT * FROM weather_data WHERE date = %s AND state = 'NY' AND data_type = 'A'", (date_str,))
                actual = cursor.fetchone()

                if actual:
                    fields = [
                        'temperature_2m_min', 'temperature_2m_max',
                        'relative_humidity_2m_max', 'precipitation_sum',
                        'wind_speed_10m_max', 'weather_code'
                    ]
                    if all(actual.get(field) is None for field in fields):
                        cursor.execute("DELETE FROM weather_data WHERE date = %s AND state = 'NY'", (date_str,))
                    else:
                        continue

                cursor.execute("DELETE FROM weather_data WHERE date = %s AND state = 'NY' AND data_type = 'P'", (date_str,))
                cursor.execute('''
                    INSERT INTO weather_data (date, state, data_type,
                        temperature_2m_min, temperature_2m_max,
                        relative_humidity_2m_max, precipitation_sum,
                        wind_speed_10m_max, weather_code)
                    VALUES (%s, %s, 'P', %s, %s, %s, %s, %s, %s)
                ''', (
                    date_str, 'NY',
                    metrics['temperature_2m_min'][i],
                    metrics['temperature_2m_max'][i],
                    metrics['relative_humidity_2m_max'][i],
                    metrics['precipitation_sum'][i],
                    metrics['wind_speed_10m_max'][i],
                    metrics['weather_code'][i],
                ))

        connection.commit()
        print(f"Inserted forecast for {len(dates)} days.")
    finally:
        connection.close()

def main():
    update_actuals()
    update_forecast()

if __name__ == "__main__":
    main()
