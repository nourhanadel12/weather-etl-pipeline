# Weather Data Project

This project is designed to fetch, store, and update historical and forecasted weather data for New York City using the Open-Meteo API and a MySQL database.

---

# Database Structure

The project uses a table called `weather_data` which stores the following fields:

- **date**: The date of the weather record  
- **state**: The U.S. state (always set to 'NY')  
- **data_type**: Either 'A' for actual data or 'P' for predicted (forecast) data  
- **temperature_2m_min**: Minimum temperature in Fahrenheit (°F)  
- **temperature_2m_max**: Maximum temperature in Fahrenheit (°F)  
- **relative_humidity_2m_max**: Maximum relative humidity in percentage (%)  
- **precipitation_sum**: Total daily precipitation in inches  
- **rain_sum**: Total daily rain in inches  
- **snowfall_sum**: Total daily snowfall in inches  
- **wind_speed_10m_max**: Maximum wind speed in miles per hour (mph)  
- **weather_code**: Numeric code representing the weather condition  

Missing values from the API will be saved as **NULL** in the database.

---

# Weather Code Meanings

- **0** → Clear  
- **1 to 3** → Partly cloudy  
- **45 to 48** → Fog  
- **51 to 57** → Drizzle  
- **61 to 67** → Rain  
- **71 to 77** → Snow  
- **80 to 82** → Rain showers  
- **85 to 86** → Snow showers  
- **95 to 99** → Thunderstorms  

---

# Required Python Libraries

- requests  
- pymysql  
- python-dotenv  
- cryptography  

---

# Scripts

1. **setup_database.py**  

2. **historical_loader.py**  
   Run the script `historical_loader.py` once to load the full historical weather dataset starting from 2015 until today.  

3. **incremental_updater.py**  
   After loading historical data once, use `incremental_updater.py` for daily updates.  

   This script performs two main actions:

   - **Update actual weather data**:  
     It checks the most recent date stored as actual data and fetches new actual records starting from the next day up to yesterday. If a prediction already exists for one of these dates, it will be replaced with the actual values.  

   - **Update forecast data**:  
     It fetches a 16-day forecast starting from today. If a date already has actual data, it skips inserting a prediction. If a prediction already exists, it is replaced with the new one.  

---

# Units Used

- **Temperature**: Fahrenheit (°F)  
- **Wind speed**: Miles per hour (mph)  
- **Precipitation, rain, and snowfall**: Inches  
- **Humidity**: Percentage (%)  

---

# Environment Variables

You should configure a `.env` file with your database credentials.  
The following variables are used:

```
DB_HOST = localhost
DB_USER = root
DB_PASSWORD = password
DB_NAME = nyc_weather
```

---

# Author

**Nourhan Adel**
