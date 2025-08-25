import requests
import psycopg2
from datetime import datetime
import logging

url = "https://disease.sh/v3/covid-19/vaccine/coverage/countries?lastdays=1"

def fetch_and_store_data():
    
    try:
        #Step 1: Fetch COVID-19 data from the API
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        rows =[]

        for item in data:
            country = item["country"]
            timeline = item["timeline"]

            for date , value in timeline.items():
                 rows.append({
                "country": country,
                "date": date,
                "value": value
            })
                 
        conn = psycopg2.connect(
            host="localhost",
            database="newDB",
            user="postgres",
            password="admin"
        )
                 
        if conn:
            print("✅ Connected to PostgreSQL successfully!")
        else:
            print("❌ Connection object is None. Something went wrong.")


    except Exception as e:
        logging.error("Error occurred: %s", str(e))
        raise

fetch_and_store_data()