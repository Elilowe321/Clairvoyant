import cfbd
import psycopg2
from cfbd.rest import ApiException
from database.database_commands import cfbd_configuration


"""
Since all the teams are determined before the season starts
and the database saves those games,
only need to call this at the start of each season
"""
def get_teams(connection):
    # Configure API key authorization: ApiKeyAuth
    configuration = cfbd_configuration()

    # create an instance of the API class
    teams_api = cfbd.TeamsApi(cfbd.ApiClient(configuration))

    try:
        # Create the "teams" table
        create_teams_table(connection)

        # Get all fbs teams
        api_response = teams_api.get_teams()

        # Insert data into the "teams" table
        insert_teams_data(connection, api_response)
        print(f"Num of Teams: {len(api_response)}")

    except ApiException as e:
        print("Exception when calling teams_api->get_teams: %s\n" % e)


# Function to create the "teams" table
def create_teams_table(connection):
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS teams (
                id INT PRIMARY KEY,
                school VARCHAR(100),
                location VARCHAR(30),
                abbreviation VARCHAR(50),
                classification VARCHAR(50),
                color VARCHAR(50),
                conference VARCHAR(500),
                division VARCHAR(50),
                twitter VARCHAR(100),
                mascot VARCHAR(100),
                alt_name_1 VARCHAR(100),
                alt_name_2 VARCHAR(100),
                alt_name_3 VARCHAR(100),
                alt_color VARCHAR(50),
                logos TEXT
            )
        """
        )
        connection.commit()
        cursor.close()

        print("Table 'teams' created successfully.")
    except psycopg2.Error as e:
        connection.rollback()

        print("Error creating 'teams' table in PostgreSQL:", e)


# Function to insert data into the "teams" table
def insert_teams_data(connection, data):
    try:
        cursor = connection.cursor()
        for team in data:
            cursor.execute(
                """
                INSERT INTO teams (id, school, location, abbreviation, classification, color, conference, division, twitter,
                mascot, alt_name_1, alt_name_2, alt_name_3, alt_color, logos)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    team.id,
                    team.school,
                    team.location.state,
                    team.abbreviation,
                    team.classification,
                    team.color,
                    team.conference,
                    team.division,
                    team.twitter,
                    team.mascot,
                    team.alt_name_1,
                    team.alt_name_2,
                    team.alt_name_3,
                    team.alt_color,
                    team.logos,
                ),
            )
        connection.commit()
        cursor.close()

        print("Data inserted successfully.")
    except psycopg2.Error as e:
        connection.rollback()

        print("Error inserting data into PostgreSQL:", e)