import cfbd
import psycopg2
from cfbd.rest import ApiException
from database.database_commands import cfbd_configuration

"""
Since all the games are determined before the season starts
and the database saves those games,
only need to call this at the start of each season

for year in range(2001, 2024): # TODO:: Not needed anymore
"""


def get_games(connection, year):

    # Configure API key authorization
    configuration = cfbd_configuration()

    # Create an instance of the API class
    games_api = cfbd.GamesApi(cfbd.ApiClient(configuration))

    try:
        # Create the "games" table
        create_games_table(connection)

        # Get games data from the API
        api_response = games_api.get_games(year=year)

        # Insert data into the "games" table
        insert_games_data(connection, api_response)

    except ApiException as e:
        print("Exception when calling teams_api->get_teams: %s\n" % e)



# Function to create the "games" table
def create_games_table(connection):
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS games (
                id SERIAL PRIMARY KEY,
                season INT,
                week INT,
                season_type VARCHAR,
                start_date VARCHAR,
                start_time_tbd BOOLEAN,
                completed BOOLEAN,
                neutral_site BOOLEAN,
                conference_game BOOLEAN,
                attendance INT,
                venue_id INT,
                venue VARCHAR,
                home_id INT,
                home_team VARCHAR,
                home_conference VARCHAR,
                home_division VARCHAR,
                home_points INT,
                home_line_scores INT[],
                home_post_win_prob FLOAT,
                home_pregame_elo INT,
                home_postgame_elo INT,
                away_id INT,
                away_team VARCHAR,
                away_conference VARCHAR,
                away_division VARCHAR,
                away_points INT,
                away_line_scores INT[],
                away_post_win_prob FLOAT,
                away_pregame_elo INT,
                away_postgame_elo INT,
                excitement_index FLOAT,
                highlights VARCHAR,
                notes VARCHAR
            )
        """
        )
        connection.commit()
        cursor.close()

        print("Table 'games' created successfully.")
    except psycopg2.Error as e:
        connection.rollback()

        print("Error creating 'games' table in PostgreSQL:", e)


# Function to insert data into the "games" table
def insert_games_data(connection, data):
    try:
        cursor = connection.cursor()
        for game in data:
            cursor.execute(
                """
                INSERT INTO games (id, season, week, season_type, start_date, start_time_tbd, completed, neutral_site,
                conference_game, attendance, venue_id, venue, home_id, home_team, home_conference, home_division,
                home_points, home_line_scores, home_post_win_prob, home_pregame_elo, home_postgame_elo, away_id,
                away_team, away_conference, away_division, away_points, away_line_scores, away_post_win_prob,
                away_pregame_elo, away_postgame_elo, excitement_index, highlights, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    game.id,
                    game.season,
                    game.week,
                    game.season_type,
                    game.start_date,
                    game.start_time_tbd,
                    game.completed,
                    game.neutral_site,
                    game.conference_game,
                    game.attendance,
                    game.venue_id,
                    game.venue,
                    game.home_id,
                    game.home_team,
                    game.home_conference,
                    game.home_division,
                    game.home_points,
                    game.home_line_scores,
                    game.home_post_win_prob,
                    game.home_pregame_elo,
                    game.home_postgame_elo,
                    game.away_id,
                    game.away_team,
                    game.away_conference,
                    game.away_division,
                    game.away_points,
                    game.away_line_scores,
                    game.away_post_win_prob,
                    game.away_pregame_elo,
                    game.away_postgame_elo,
                    game.excitement_index,
                    game.highlights,
                    game.notes,
                ),
            )
        connection.commit()
        cursor.close()

        print("Data inserted successfully.")
    except psycopg2.Error as e:
        connection.rollback()

        print("Error inserting data into PostgreSQL:", e)