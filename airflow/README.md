# Airflow Weather Data Pipelines

This folder contains Apache Airflow DAGs for fetching, transforming, and loading weather data 
from the Open-Meteo API into a MySQL database.

##  Included DAGs
- **load_historical_weather_split_tasks.py**  
  Fetches full historical weather data (from 2015 to today), transforms it, and loads it into MySQL.  

- **incremental_update_weather_split.py**  
  Performs incremental updates by:  
  1. Checking the last available actual record in the database.  
  2. Fetching new historical + forecast data.  
  3. Inserting or replacing records in MySQL.  

##  Requirements
Install dependencies before running Airflow:
```bash
pip install -r requirements.txt
