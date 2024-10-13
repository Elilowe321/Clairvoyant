import psycopg2
import os
from dotenv import load_dotenv
import cfbd
from decimal import Decimal



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


def get_team_average_stats(connection, columns, team_id, year, prefix):
    try:
        # Construct lists for home and away columns
        home_columns = [col for col in columns if col.startswith("home_")]
        away_columns = [col for col in columns if col.startswith("away_")]

        cursor = connection.cursor()

        # Query home and away averages in a single query for each
        home_avg_query = f"""
            SELECT {', '.join([f"AVG({col}) AS {prefix}{col.split('_', 1)[1]}" for col in home_columns])}
            FROM (
                SELECT 
                    tgs.*,
                    COALESCE(home_team.talent, 50) AS home_talent,
                    COALESCE(away_team.talent, 50) AS away_talent
                FROM 
                    team_game_stats tgs
                LEFT JOIN (
                    SELECT 
                        t.id, 
                        tt.talent, 
                        tt.year
                    FROM 
                        team_talent tt
                    JOIN 
                        teams t ON t.school = tt.school
                ) home_team ON (home_team.id = tgs.home_school_id) AND (home_team.year = tgs.year)
                LEFT JOIN (
                    SELECT 
                        t.id, 
                        tt.talent, 
                        tt.year
                    FROM 
                        team_talent tt
                    JOIN 
                        teams t ON t.school = tt.school
                ) away_team ON (away_team.id = tgs.away_school_id) AND (away_team.year = tgs.year)
            )
            WHERE home_school_id = {team_id}
            AND year = {year};
        """
        cursor.execute(home_avg_query)
        column_names = [desc[0] for desc in cursor.description]
        home_avgs = cursor.fetchone()
        home_avg_with_names = dict(zip(column_names, home_avgs))

        away_avg_query = f"""
            SELECT {', '.join([f"AVG({col}) AS {prefix}{col.split('_', 1)[1]}" for col in away_columns])}
            FROM (
                SELECT 
                    tgs.*,
                    COALESCE(home_team.talent, 50) AS home_talent,
                    COALESCE(away_team.talent, 50) AS away_talent
                FROM 
                    team_game_stats tgs
                LEFT JOIN (
                    SELECT 
                        t.id, 
                        tt.talent, 
                        tt.year
                    FROM 
                        team_talent tt
                    JOIN 
                        teams t ON t.school = tt.school
                ) home_team ON (home_team.id = tgs.home_school_id) AND (home_team.year = tgs.year)
                LEFT JOIN (
                    SELECT 
                        t.id, 
                        tt.talent, 
                        tt.year
                    FROM 
                        team_talent tt
                    JOIN 
                        teams t ON t.school = tt.school
                ) away_team ON (away_team.id = tgs.away_school_id) AND (away_team.year = tgs.year)
            )
            WHERE away_school_id = {team_id}
            AND year = {year};
        """
        cursor.execute(away_avg_query)
        column_names = [desc[0] for desc in cursor.description]
        away_avgs = cursor.fetchone()
        away_avg_with_names = dict(zip(column_names, away_avgs))

        # Combine home and away averages
        averages = {}
        for key in home_avg_with_names:
            home_value = home_avg_with_names[key]
            away_value = away_avg_with_names.get(key)

            if isinstance(home_value, Decimal):
                home_value = float(home_value)
            if isinstance(away_value, Decimal):
                away_value = float(away_value)

            if home_value is not None and away_value is not None:
                averages[key] = (home_value + away_value) / 2
            elif home_value is not None:
                averages[key] = home_value
            elif away_value is not None:
                averages[key] = away_value
            else:
                averages[key] = 0
        
        
        # print("ID: ", team_id)
        # print(averages)
        

        return averages

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    


def game_lines(connection, game_id):
    try:
        cursor = connection.cursor()

        cursor.execute(
            f"""
            Select *
                from betting_lines
                where game_id = {game_id};
        """
        )
        rows = cursor.fetchall()
        connection.commit()
        cursor.close()

        return rows

    except psycopg2.Error as e:
        connection.rollback()

        print("Error getting 'game_lines' table in PostgreSQL:", e)