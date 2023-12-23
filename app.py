import os
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv
from flask import Flask, request

load_dotenv()

app = Flask(__name__)
db_url = os.getenv("DATABASE_URL")
db_connection = psycopg2.connect(db_url)

# SQL statements
CREATE_ZONES_TABLE = "CREATE TABLE IF NOT EXISTS zones (id SERIAL PRIMARY KEY, name TEXT);"
CREATE_TEMPS_TABLE = """CREATE TABLE IF NOT EXISTS temperatures (zone_id INTEGER, temperature REAL NOT NULL, 
                        date TIMESTAMP, FOREIGN KEY(zone_id) REFERENCES zones(id) ON DELETE CASCADE);"""
CREATE_LIGHT_TABLE = """CREATE TABLE IF NOT EXISTS luminosities (zone_id INTEGER, luminosity NUMERIC(3, 1) NOT NULL, 
                        date TIMESTAMP, FOREIGN KEY(zone_id) REFERENCES zones(id) ON DELETE CASCADE);"""
CREATE_HUMIDITY_TABLE = """CREATE TABLE IF NOT EXISTS humidity (zone_id INTEGER, humidity NUMERIC(3, 1) NOT NULL, 
                        date TIMESTAMP, FOREIGN KEY(zone_id) REFERENCES zones(id) ON DELETE CASCADE);"""
INSERT_ZONE_RETURN_ID = "INSERT INTO zones (name) VALUES (%s) RETURNING id;"
INSERT_TEMP = "INSERT INTO temperatures (zone_id, temperature, date) VALUES (%s, %s, %s);"
INSERT_LIGHT = "INSERT INTO luminosities (zone_id, luminosity, date) VALUES (%s, %s, %s);"
INSERT_HUMID = "INSERT INTO humidity (zone_id, humidity, date) VALUES (%s, %s, %s);"

# SQL Averages
DAYS_TEMP_RECORDING = "SELECT COUNT(DISTINCT DATE(date)) FROM temperatures;"
AVG_TEMP = "SELECT AVG(temperature) FROM temperatures;"
AVG_HUMID = "SELECT AVG(humidity) FROM humidity"
AVG_LIGHT = "SELECT AVG(luminosity) FROM luminosities"
ZONE_NAME = "SELECT name FROM zones WHERE id = (%s)"
NUM_OF_ZONE_TEMP = "SELECT COUNT(DISTINCT DATE(date)) AS days FROM temperatures WHERE zone_id = (%s);"
ZONE_AVG_TEMP = "SELECT AVG(temperature) as average FROM temperatures WHERE zone_id = (%s);"
ZONE_AVG_HUMID = "SELECT AVG(humidity) as average FROM humidity WHERE zone_id = (%s);"
ZONE_AVG_LIGHT = "SELECT AVG(luminosity) as average FROM luminosities WHERE zone_id = (%s);"


@app.post("/api/zone/")
def create_zone():
    data = request.get_json()
    name = data["name"]

    with db_connection:
        with db_connection.cursor() as cursor:
            cursor.execute(CREATE_ZONES_TABLE)
            cursor.execute(INSERT_ZONE_RETURN_ID, (name,))
            zone_id = cursor.fetchone()[0]
        return {"id": zone_id, "message": f"Zone {name} Zone created."}, 201


@app.post("/api/temperature/")
def add_temp_data():
    data = request.get_json()
    zone_id = data["zone"]
    temperature = data["temperature"]
    try:
        date = datetime.strptime(data["date"], "%m-%d-%Y %H:%M:%S")
    except KeyError:
        date = datetime.now(timezone.utc)
    with db_connection:
        with db_connection.cursor() as cursor:
            cursor.execute(CREATE_TEMPS_TABLE)
            cursor.execute(INSERT_TEMP, (zone_id, temperature, date))
    return {"message": "Temperature stored successfully."}, 201


@app.get("/api/temp/average/")
def get_temp_avg():
    with db_connection:
        with db_connection.cursor() as cursor:
            cursor.execute(AVG_TEMP)
            average = cursor.fetchone()[0]
            cursor.execute(DAYS_TEMP_RECORDING)
            days = cursor.fetchone()[0]
    return {"temperature_average": round(average, 2), "days": days}


@app.get("/api/zone/<int:zone_id>/")
def get_zone_data(zone_id):
    with db_connection:
        with db_connection.cursor() as cursor:
            cursor.execute(ZONE_NAME, (zone_id,))
            name = cursor.fetchone()[0]
            cursor.execute(ZONE_AVG_TEMP, (zone_id,))
            average = cursor.fetchone()[0]
            cursor.execute(DAYS_TEMP_RECORDING, (zone_id,))
            number_of_days = cursor.fetchone()[0]
    return {"zone_name": name, "average": round(average, 2), "number_of_days": number_of_days}
