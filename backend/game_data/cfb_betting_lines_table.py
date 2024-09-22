import cfbd
import psycopg2
from cfbd.rest import ApiException
from database.database_commands import cfbd_configuration


def get_betting_lines(connection, year, week):
    # Configure API key authorization
    configuration = cfbd_configuration()

    # create an instance of the API class
    api_instance = cfbd.BettingApi(cfbd.ApiClient(configuration))

    try:

        # Create betting table
        create_betting_lines_table(connection)

        # For each team get all data
        api_response = api_instance.get_lines(year=year, week=week)

        insert_betting_lines_data(connection, api_response)

    except ApiException as e:
        print("Exception when calling BettingApi->get_lines: %s\n" % e)



# Function to create the "betting_lines" table
def create_betting_lines_table(connection):
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS betting_lines (
                game_id INT PRIMARY KEY,
                home_moneyline FLOAT NULL,
                away_moneyline FLOAT NULL,
                spread FLOAT NULL,
                over_under FLOAT NULL,
                provider VARCHAR
            )
        """
        )
        connection.commit()
        cursor.close()

        print("Table 'betting_lines' created successfully.")
    except psycopg2.Error as e:
        connection.rollback()

        print("Error creating 'betting_lines' table in PostgreSQL:", e)


# Function to insert or update data in the "betting_lines" table
def insert_betting_lines_data(connection, data):
    try:
        cursor = connection.cursor()

        for line in data:
            for bets in line.lines:
                if bets.home_moneyline is not None:
                    cursor.execute(
                        """
                        INSERT INTO betting_lines (game_id, home_moneyline, away_moneyline, spread, over_under, provider)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (game_id) DO UPDATE SET
                            home_moneyline = EXCLUDED.home_moneyline,
                            away_moneyline = EXCLUDED.away_moneyline,
                            spread = EXCLUDED.spread,
                            over_under = EXCLUDED.over_under,
                            provider = EXCLUDED.provider
                        """,
                        (
                            line.id,
                            bets.home_moneyline,
                            bets.away_moneyline,
                            bets.spread,
                            bets.over_under,
                            bets.provider,
                        ),
                    )
                    break

        connection.commit()
        cursor.close()
    except psycopg2.Error as e:
        connection.rollback()
        print("Error inserting/updating data into PostgreSQL:", e)