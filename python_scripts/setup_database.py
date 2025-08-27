import pymysql
from pymysql import cursors
from dotenv import load_dotenv
import os
load_dotenv()

def setup_database():
    connection = None
    try:
        connection = pymysql.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            charset='utf8mb4',
            cursorclass=cursors.DictCursor
        )

        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {os.getenv('DB_NAME')}")
            cursor.execute(f"USE {os.getenv('DB_NAME')}")
            cursor.execute("DROP TABLE IF EXISTS weather_data")

            cursor.execute('''
                CREATE TABLE weather_data
                (
                    date                     DATE,
                    state                    VARCHAR(2) DEFAULT 'NY',
                    data_type                CHAR(1),
                    temperature_2m_min       DECIMAL(5, 2),
                    temperature_2m_max       DECIMAL(5, 2),
                    relative_humidity_2m_max DECIMAL(5, 2),
                    precipitation_sum        DECIMAL(5, 2),
                    rain_sum                 DECIMAL(5, 2),
                    snowfall_sum             DECIMAL(5, 2),
                    wind_speed_10m_max       DECIMAL(5, 2),
                    weather_code             INT,
                    PRIMARY KEY (date, state),
                    CONSTRAINT chk_data_type CHECK (data_type IN ('A', 'P'))
                )''')
        connection.commit()
        print("Database setup completed successfully")

    except pymysql.MySQLError as e:
        print(f"Database error: {e}")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    setup_database()