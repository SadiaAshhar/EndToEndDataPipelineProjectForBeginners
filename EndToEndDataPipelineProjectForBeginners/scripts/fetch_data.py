import requests
import psycopg2
from datetime import datetime
import logging

url = "https://disease.sh/v3/covid-19/vaccine/coverage/countries?lastdays=1"

def covid_vaccine_data():
    
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

        #Step 2: Connect of postgresDB    
        conn = psycopg2.connect(
            host="localhost",
            database="newDB",
            user="postgres",
            password="admin"
        )
                 
        cursor = conn.cursor()
        
        # Step 3: Using if not exists to create tble if it does not exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS covid_vaccine_data (
            id SERIAL PRIMARY KEY,
            country_name VARCHAR(100),
            date VARCHAR(100),
            value INT
        );
        """
        cursor.execute(create_table_query)

        delete_existing_records = """
        DELETE FROM covid_vaccine_data;        
        """
        cursor.execute(delete_existing_records)
        insert_data(rows, conn)
        

    except Exception as e:
        logging.error("Error occurred: %s", str(e))
        raise



def insert_data(rows, conn):
    try:
        
        insert_query = """
            INSERT INTO covid_vaccine_data (country_name, date, value)
            VALUES (%s, %s, %s)
        """

        values = [(row["country"], row["date"], row["value"]) for row in rows]
        cursor = conn.cursor()

        cursor.executemany(insert_query, values)
        conn.commit()
        print(f"✅ Inserted {cursor.rowcount} rows successfully.")

        cursor.close()
        conn.close()

    except Exception as e:
        print("❌ Error while inserting:", e)


