import psycopg2
import os
from dotenv import load_dotenv
import cfbd


# Load environment vars from .env
load_dotenv()

# Configure to cfbd api
def cfbd_configuration():

    configuration = cfbd.Configuration()
    configuration.api_key["Authorization"] = (
        "kjsyUDd6vFuPIPL6wDhlGPDrYILbYlZDoUo64iE8c8qtSLHwL6pX/iSEzz5fxbhJ"
    )
    configuration.api_key_prefix["Authorization"] = "Bearer"

    return configuration


# Function to create a connection to the PostgreSQL database
def create_connection():
    try:

        connection = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            # host="localhost",
            port=os.getenv("DB_PORT"),
        )
        return connection
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        return None
