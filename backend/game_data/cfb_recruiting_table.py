import cfbd
import psycopg2
from cfbd.rest import ApiException
from database.database_commands import cfbd_configuration


"""
Gets Recruiting rankings for a team in a specific year
"""
def get_team_recruiting(connection, year):

    # Configure API key authorization: ApiKeyAuth
    configuration = cfbd_configuration()

    # create an instance of the API class
    api_instance = cfbd.RecruitingApi(cfbd.ApiClient(configuration))

    try:
        # Create recruting table
        create_recruiting_table(connection)

        api_response = api_instance.get_recruiting_teams(year=year)
        # print(api_response)
        insert_recruiting_data(connection, api_response)

    except ApiException as e:
        print("Exception when calling RecruitingApi->get_recruiting_teams: %s\n" % e)



# Function to create the "recruiting" table
def create_recruiting_table(connection):
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS recruiting (
                points FLOAT,
                rank INT,
                team VARCHAR,
                year INT
            )
        """
        )
        connection.commit()
        cursor.close()

        print("Table 'recruiting' created successfully.")
    except psycopg2.Error as e:
        connection.rollback()

        print("Error creating 'recruiting' table in PostgreSQL:", e)


# Function to insert data into the "recruiting" table
def insert_recruiting_data(connection, data):
    try:
        cursor = connection.cursor()

        for team in data:

            if team.team == "Hawai'i":
                team.team = "Hawaii"

            # Make sure not to run the same year twice and get duplicates
            tableName = "recruiting"
            cursor.execute(
                f"""SELECT 1 
                    FROM {tableName} 
                    WHERE team = '{team.team}' AND year = {team.year}
                    """
            )

            exists = cursor.fetchone()

            # Insert only if the combination does not exist
            if not exists:
                cursor.execute(
                    """
                    INSERT INTO recruiting (points, rank, team, year)
                    VALUES (%s, %s, %s, %s)
                """,
                    (team.points, team.rank, team.team, team.year),
                )
                print("Data inserted successfully.")

        connection.commit()
        cursor.close()

    except psycopg2.Error as e:
        connection.rollback()

        print("Error inserting data into PostgreSQL:", e)
