##############################
# Initial test to make sure  #
# docker database is working #
##############################

import psycopg2
import os
from dotenv import load_dotenv

# load env variables
load_dotenv()

# Create database connection
def create_connection():
    try:
        connection = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )
        return connection
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        return None

connection = create_connection()

if connection:
    try:

        # Create test table to see if it works
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS test_table (
                id INT
            );
            """
        )
        connection.commit()
        cursor.close()
        print("Table 'test_table' created successfully.")
        
    except psycopg2.Error as e:
        connection.rollback()
        print("Error creating 'test_table' table in PostgreSQL:", e)
    finally:
        connection.close()
