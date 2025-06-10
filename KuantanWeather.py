import requests
import pandas as pd
import time
import os
import mysql.connector
from mysql.connector import Error

# API key and city info
API_KEY = "b54c29e1ec7b1b8077dff443ee27d180"
CITY = "Kuantan"
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

# MySQL database connection info
DB_HOST = "your-database-host"  # For example, "localhost" or "your-mysql-host.com"
DB_NAME = "weather_data"        # Your database name
DB_USER = "your-username"       # Your MySQL username
DB_PASSWORD = "your-password"   # Your MySQL password

# Create a MySQL connection
def create_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        if connection.is_connected():
            print("Connected to MySQL database")
    except Error as e:
        print(f"Error: {e}")
    return connection

# Function to insert data into MySQL
def insert_weather_data(timestamp, temperature, humidity, wind_speed):
    connection = create_connection()
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO weather (timestamp, temperature, humidity, wind_speed)
    VALUES (%s, %s, %s, %s)
    """
    data = (timestamp, temperature, humidity, wind_speed)
    cursor.execute(insert_query, data)
    connection.commit()
    cursor.close()
    connection.close()
    print(f"Data inserted at {timestamp}")

# Main loop to collect weather data every 5 minutes
for _ in range(5):  # Collect 5 samples
    response = requests.get(URL)
    data = response.json()

    temp = data["main"]["temp"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]

    timestamp = pd.Timestamp.now()

    # Insert the collected data into MySQL database
    insert_weather_data(timestamp, temp, humidity, wind_speed)

    print(f"Logged: {timestamp} - Temp: {temp}°C, Humidity: {humidity}%, Wind Speed: {wind_speed} m/s")

    # Wait for 5 minutes before next request
    time.sleep(300)  # Wait 5 minutes (300 seconds)
