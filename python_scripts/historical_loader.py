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


def get_safe_date_range(year):
    today = date.today()
    if year > today.year:
        return None, None
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31" if year < today.year else today.strftime('%Y-%m-%d')
    return start_date, end_date


def fetch_weather_data(start_date, end_date):
    if not start_date:
        return None

    variable_groups = [
        "temperature_2m_max,temperature_2m_min,relative_humidity_2m_max",
        "precipitation_sum,rain_sum,snowfall_sum",
        "wind_speed_10m_max",
        "weather_code"
    ]

    combined_data = {'daily': {}}
    max_retries = 3
    retry_count = 0
    original_end_date = end_date

    while retry_count <= max_retries:
        try:
            for group in variable_groups:
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
                    timeout=100
                )
                response.raise_for_status()
                data = response.json()

                if not combined_data['daily'].get('time'):
                    combined_data['daily']['time'] = data['daily']['time']

                for key in data['daily']:
                    if key != 'time':
                        combined_data['daily'][key] = data['daily'][key]

            return combined_data if combined_data['daily'].get('time') else None

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400 and retry_count < max_retries:
                retry_count += 1
                end_date = (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
                print(f"Received 400 error, retrying with end date: {end_date} (attempt {retry_count}/{max_retries})")
                continue
            print(f"Failed to fetch data for {start_date} to {original_end_date}: {str(e)}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data: {str(e)}")
            return None

    return None


def save_weather_data(data, year):
    if not data or 'daily' not in data:
        print(f"No valid data for {year}")
        return False

    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            dates = data['daily']['time']
            metrics = data['daily']

            for i, date_str in enumerate(dates):
                cursor.execute(
                    "SELECT data_type FROM weather_data WHERE date = %s AND state = 'NY'",
                    (date_str,)
                )
                row = cursor.fetchone()

                if row:
                    if row['data_type'] == 'P':
                        cursor.execute(
                            "DELETE FROM weather_data WHERE date = %s AND state = 'NY'",
                            (date_str,)
                        )
                    else:
                        continue

                values = (
                    date_str, 'NY', 'A',
                    metrics.get('temperature_2m_min', [None] * len(dates))[i],
                    metrics.get('temperature_2m_max', [None] * len(dates))[i],
                    metrics.get('relative_humidity_2m_max', [None] * len(dates))[i],
                    metrics.get('precipitation_sum', [None] * len(dates))[i],
                    metrics.get('rain_sum', [None] * len(dates))[i],
                    metrics.get('snowfall_sum', [None] * len(dates))[i],
                    metrics.get('wind_speed_10m_max', [None] * len(dates))[i],
                    metrics.get('weather_code', [None] * len(dates))[i]
                )

                cursor.execute('''
                               INSERT INTO weather_data (date, state, data_type,
                                                         temperature_2m_min, temperature_2m_max,
                                                         relative_humidity_2m_max, precipitation_sum,
                                                         rain_sum, snowfall_sum, wind_speed_10m_max,
                                                         weather_code)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                               ''', values)

            connection.commit()
            print(f"Saved {len(dates)} days for {year}")
            return True

    except pymysql.MySQLError as e:
        print(f"Database error for {year}: {e}")
        return False
    finally:
        if connection:
            connection.close()


def process_historical_data():
    current_year = date.today().year
    successful_years = 0

    for year in range(2015, current_year + 1):
        start_date, end_date = get_safe_date_range(year)

        if not start_date:
            print(f"Skipping future year {year}")
            continue

        print(f"Processing {year} ({start_date} to {end_date})")

        data = fetch_weather_data(start_date, end_date)
        if data and save_weather_data(data, year):
            successful_years += 1

    print(f"\nSuccessfully processed {successful_years} years")


if __name__ == "__main__":
    process_historical_data()