import cfbd
import psycopg2
from cfbd.rest import ApiException
from database.database_commands import cfbd_configuration


"""
Run Once a year
"""
def get_team_talent(connection, year):
    # Configure API key authorization: ApiKeyAuth
    configuration = cfbd_configuration()

    # Create an instance of the API class
    api_instance = cfbd.TeamsApi(cfbd.ApiClient(configuration))

    try:

        # Create table
        create_team_talent_table(connection)

        # Team talent composite rankings for a given year
        api_response = api_instance.get_talent(year=year)

        for school in api_response:
            print(school.school, school.talent, school.year)
            insert_team_talent_data(connection, school)

    except ApiException as e:
        print("Exception when calling TeamsApi->get_talent: %s\n" % e)


# Function to create the "team_talent" table
def create_team_talent_table(connection):
    try:
        cursor = connection.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS team_talent (
                school VARCHAR,
                talent FLOAT,
                year INT
            )
        """
        )

        connection.commit()
        cursor.close()

        print("Table 'team_talent' created successfully.")
    except psycopg2.Error as e:
        connection.rollback()

        print("Error creating 'team_talent' table in PostgreSQL:", e)


# Function to insert data into the "team_talent" table
def insert_team_talent_data(connection, data):
    try:
        cursor = connection.cursor()

        # Make sure not to run the same year twice and get duplicates
        tableName = "team_talent"
        cursor.execute(
            f"""SELECT 1 
                        FROM {tableName} 
                        WHERE school = '{data.school}' AND year = '{data.year}'
                        """
        )

        exists = cursor.fetchone()

        # Insert only if the combination does not exist
        if not exists:
            cursor.execute(
                """
                INSERT INTO team_talent (school, talent, year)
                VALUES (%s, %s, %s)
            """,
                (data.school, data.talent, data.year),
            )
            connection.commit()

        connection.commit()
        cursor.close()

    except psycopg2.Error as e:
        connection.rollback()

        print("Error inserting data into PostgreSQL:", e)