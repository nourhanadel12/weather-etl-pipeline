# Airbyte Weather Data Pipelines

This folder contains Airbyte configurations for fetching weather data from the 
[Open-Meteo API](https://open-meteo.com/) and loading it into a MySQL database.

##  Files
- sources/historical_weather_nyc.yaml
  Declarative source connector for historical weather in New York City (from 2015 onward).  

- sources/forecast_weather_nyc.yaml  
  Declarative source connector for 16-day weather forecast in New York City.  

- destinations/mysql_destination.json
  Destination configuration for MySQL where all weather data is stored.

##  Usage
1. Open your Airbyte UI.  
2. Import sources:  
   - Go to Sources → Import Source and upload the YAML files.  
3. Import the destination:  
   - Go to Destinations → Import Destination and upload mysql_destination.json.  
4. Create a connection in Airbyte UI:  
   - Source = historical_weather_nyc or forecast_weather_nyc.  
   - Destination = MySQL Weather Data.   
5. Run the sync to fetch and load the data.

## ️ Database
Data will be written into the nyc_weather database in MySQL, under automatically generated tables 
(e.g., `historical_weather_nyc`, `forecast_weather_nyc`).  
