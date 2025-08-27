# Weather Data Pipeline 

This project demonstrates how to build an **end-to-end data pipeline** for weather data using multiple tools:  
- **Python scripts** → simple standalone ETL jobs  
- **Airflow** → DAG-based orchestration  
- **Airbyte** → declarative source/destination connectors  
- **NiFi** → drag-and-drop flow-based ETL  

All pipelines fetch data from the **Open-Meteo API** (historical + forecast) and load it into a **MySQL database**.

---

## Repository Structure
```
weather-data-pipeline/
 ├── airflow/           # Airflow DAGs, requirements, README
 ├── airbyte/           # Airbyte sources/destination configs
 ├── nifi/              # NiFi flow definitions
 ├── python_scripts/    # Standalone Python ETL scripts
 └── README.md          # Main project documentation
```

---

##  How It Works
1. **Open-Meteo API** is the data source (historical + forecast).  
2. Data is **transformed** (fields cleaned, state and data type added).  
3. Data is **loaded** into a MySQL table: `weather_data`.  

Supported approaches:
- **Python scripts**: simple one-off jobs.  
- **Airflow**: DAG orchestration with retries and scheduling.  
- **Airbyte**: low-code declarative connectors.  
- **NiFi**: flow-based ETL with visual management.  

---

##  Database Schema
```sql
CREATE TABLE weather_data (
    date DATE NOT NULL,
    state VARCHAR(10),
    data_type CHAR(1), -- 'A' for Actual, 'P' for Prediction
    temperature_2m_min FLOAT,
    temperature_2m_max FLOAT,
    relative_humidity_2m_max FLOAT,
    precipitation_sum FLOAT,
    rain_sum FLOAT,
    snowfall_sum FLOAT,
    wind_speed_10m_max FLOAT,
    weather_code INT,
    PRIMARY KEY (date, state)
);
```

---

##  Why This Project?
This repository shows how the **same ETL pipeline** can be implemented in different tools, useful for:  
- Comparing orchestration frameworks (Airflow vs NiFi vs Airbyte).  
- Demonstrating end-to-end data engineering workflows.  
- Practicing ETL best practices with real-world weather data.

---

##  Tools & Versions
- Python 3.9+  
- Apache Airflow 2.x  
- Apache NiFi 1.23+  
- Airbyte 0.50+  
- MySQL 8+  

---

