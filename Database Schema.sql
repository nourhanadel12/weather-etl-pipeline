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