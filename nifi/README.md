# NiFi Weather Data Flows

This folder contains Apache NiFi process group definitions for extracting weather data from the Open-Meteo API, transforming it, and loading it into a MySQL database.

---

# Included Files

- **FINAL_Historical_Open_Meteo.json** → Process group for historical weather data (from 2015 until today).  
- **Incremental_Weather_Update.json** → Process group for incremental/ongoing updates (new daily actual + forecast data).  

---

# Required Configuration

These flows rely on external services. You must configure them in your NiFi instance:

- **DBCPConnectionPool**  
  - Database: MySQL  
  - Host: host.docker.internal (or your DB host)  
  - Port: 3306  
  - Database name: nyc_weather  
  - Username: root (adjust as needed)  
  - Driver: `com.mysql.cj.jdbc.Driver`  

- **Record Reader/Writer :**  
  - JsonTreeReader or JsonPathReader for parsing API responses.  
  - JsonRecordSetWriter or AvroRecordSetWriter for transformation.  

---

# Flow Descriptions

### Historical Flow (`FINAL_Historical_Open_Meteo.json`)  
- Calls the `archive-api.open-meteo.com` endpoint.  
- Retrieves daily historical weather data from 2015 until the current date.  
- Transforms JSON into structured records.  
- Inserts results into MySQL (`weather_data` table).  

### Incremental Flow (`Incremental_Weather_Update.json`)  
- Queries the latest available date in the database.  
- Calls both:  
  - `archive-api.open-meteo.com` (for new actuals)  
  - `api.open-meteo.com/forecast` (for upcoming 16-day forecasts).  
- Merges results and upserts (insert or replace) into MySQL.  
