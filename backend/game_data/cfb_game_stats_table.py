import cfbd
import psycopg2
from cfbd.rest import ApiException
from database.database_commands import cfbd_configuration

"""
Need to run each week (besides first week)
to get updated team stats for every game.
"""
def get_game_stats(connection, year, week):
    # Configure API key authorization: ApiKeyAuth
    configuration = cfbd_configuration()

    # create an instance of the API class
    games_api = cfbd.GamesApi(cfbd.ApiClient(configuration))

    try:
        # Create the "games" table
        create_game_stats_table(connection)

        api_response = games_api.get_team_game_stats(
            year=year, week=week
        )

        insert_game_stats_data(connection, api_response, year)

    except ApiException as e:
        print("Exception when calling teams_api->get_fbs_teams: %s\n" % e)



# Function to create the "game_stats" table
def create_game_stats_table(connection):
    try:
        cursor = connection.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS team_game_stats (
                game_id INT,
                year INT,
                home_school_id INT,
                home_home_away VARCHAR,
                home_points INT,
                home_rushingAttempts FLOAT NULL,
                home_rushingYards FLOAT NULL,
                home_yardsPerPass FLOAT NULL,
                home_completionPercentage FLOAT NULL, 
                home_netPassingYards FLOAT NULL,
                home_totalYards FLOAT NULL,
                home_fourthDownEff FLOAT NULL,
                home_thirdDownEff FLOAT NULL,
                home_firstDowns FLOAT NULL,
                home_rushingTDs FLOAT NULL,
                home_puntReturnYards FLOAT NULL,
                home_puntReturnTDs FLOAT NULL,
                home_puntReturns FLOAT NULL,
                home_passingTDs FLOAT NULL,
                home_kickReturnYards FLOAT NULL,
                home_kickReturnTDs FLOAT NULL,
                home_kickReturns FLOAT NULL,
                home_kickingPoints FLOAT NULL,
                home_interceptionYards FLOAT NULL,
                home_interceptionTDs FLOAT NULL,
                home_passesIntercepted FLOAT NULL,
                home_fumblesRecovered FLOAT NULL,
                home_totalFumbles FLOAT NULL,
                home_tacklesForLoss FLOAT NULL,
                home_defensiveTDs FLOAT NULL,
                home_tackles FLOAT NULL,
                home_sacks FLOAT NULL,
                home_qbHurries FLOAT NULL,
                home_passesDeflected FLOAT NULL,
                home_possessionTime FLOAT NULL,
                home_interceptions FLOAT NULL,
                home_fumblesLost FLOAT NULL,
                home_turnovers FLOAT NULL,
                home_totalPenaltiesYards FLOAT NULL,
                home_yardsPerRushAttempt FLOAT NULL,
                away_school_id FLOAT,
                away_home_away VARCHAR,
                away_points FLOAT,
                away_rushingAttempts FLOAT NULL,
                away_rushingYards FLOAT NULL,
                away_yardsPerPass FLOAT NULL,
                away_completionPercentage FLOAT NULL, 
                away_netPassingYards FLOAT NULL,
                away_totalYards FLOAT NULL,
                away_fourthDownEff FLOAT NULL,
                away_thirdDownEff FLOAT NULL,
                away_firstDowns FLOAT NULL,
                away_rushingTDs FLOAT NULL,
                away_puntReturnYards FLOAT NULL,
                away_puntReturnTDs FLOAT NULL,
                away_puntReturns FLOAT NULL,
                away_passingTDs FLOAT NULL,
                away_kickReturnYards FLOAT NULL,
                away_kickReturnTDs FLOAT NULL,
                away_kickReturns FLOAT NULL,
                away_kickingPoints FLOAT NULL,
                away_interceptionYards FLOAT NULL,
                away_interceptionTDs FLOAT NULL,
                away_passesIntercepted FLOAT NULL,
                away_fumblesRecovered FLOAT NULL,
                away_totalFumbles FLOAT NULL,
                away_tacklesForLoss FLOAT NULL,
                away_defensiveTDs FLOAT NULL,
                away_tackles FLOAT NULL,
                away_sacks FLOAT NULL,
                away_qbHurries FLOAT NULL,
                away_passesDeflected FLOAT NULL,
                away_possessionTime FLOAT NULL,
                away_interceptions FLOAT NULL,
                away_fumblesLost FLOAT NULL,
                away_turnovers FLOAT NULL,
                away_totalPenaltiesYards FLOAT NULL,
                away_yardsPerRushAttempt FLOAT NULL
            )
        """
        )

        connection.commit()
        cursor.close()

        print("Table 'game_stats' created successfully.")
    except psycopg2.Error as e:
        connection.rollback()

        print("Error creating 'game_stats' table in PostgreSQL:", e)


# Function to insert data into the game_stats table
def insert_game_stats_data(connection, data, year):
    try:
        cursor = connection.cursor()

        # Go through every game
        for game in data:

            # Check if game was already added
            tableName = "team_game_stats"
            cursor.execute(
                f"""SELECT game_id
                            FROM {tableName}
                            """
            )

            # Fetch all rows
            rows = cursor.fetchall()

            # If the game has not been added, add it
            if game.id not in [row[0] for row in rows]:

                # Make dictonary for easy sql input
                game_data_dict = {}
                game_data_dict["game_id"] = game.id
                game_data_dict["year"] = year

                # In every game, go through each of the teams that played
                for team in game.teams:

                    if team.home_away == "home":
                        prefix = "home_"
                    else:
                        prefix = "away_"

                    # cat = f"{prefix}{team.stats[i].category}"

                    game_data_dict[f"{prefix}school_id"] = team.school_id
                    game_data_dict[f"{prefix}home_away"] = team.home_away
                    game_data_dict[f"{prefix}points"] = team.points

                    # For every team, go through each of the teams stats
                    for i in range(len(team.stats)):
                        cat = f"{prefix}{team.stats[i].category}"

                        # Change the format of completionAttempts and put it into percentage
                        if team.stats[i].category == "completionAttempts":
                            completions = int(team.stats[i].stat.split("-")[0])
                            attempts = int(team.stats[i].stat.split("-")[1])
                            if attempts == 0:
                                completion_percentage = None
                            else:
                                completion_percentage = (completions / attempts) * 100

                            game_data_dict[f"{prefix}completionPercentage"] = (
                                completion_percentage
                            )

                        # Change the format of thirdDownEff and put into percentage
                        elif team.stats[i].category == "thirdDownEff":
                            try:
                                completions = int(team.stats[i].stat.split("-")[0])
                                attempts = int(team.stats[i].stat.split("-")[1])
                                if attempts == 0:
                                    thirdDownEff = None
                                else:
                                    thirdDownEff = (completions / attempts) * 100

                                game_data_dict[cat] = thirdDownEff
                            except:
                                print("Error getting thirdDownEff")

                        # Change the format of fourthDownEff and put into percentage
                        elif team.stats[i].category == "fourthDownEff":
                            try:
                                completions = int(team.stats[i].stat.split("-")[0])
                                attempts = int(team.stats[i].stat.split("-")[1])
                                if attempts == 0:
                                    fourthDownEff = None
                                else:
                                    fourthDownEff = (completions / attempts) * 100

                                game_data_dict[cat] = fourthDownEff
                            except:
                                print("Error getting fourthDownEff")

                        # Change the format of totalPenaltiesYards
                        elif team.stats[i].category == "totalPenaltiesYards":
                            try:
                                totalPenaltiesYards = int(
                                    team.stats[i].stat.split("-")[1]
                                )
                                game_data_dict[cat] = totalPenaltiesYards

                            except:
                                print("Error getting totalPenaltiesYards")

                        # Format the time
                        elif team.stats[i].category == "possessionTime":
                            minutes = int(team.stats[i].stat.split(":")[0]) * 60
                            total_time = int(team.stats[i].stat.split(":")[1]) + minutes

                            game_data_dict[cat] = total_time

                        else:
                            game_data_dict[cat] = team.stats[i].stat

                # Generate the list of columns and corresponding values
                columns = ", ".join(game_data_dict.keys())
                placeholders = ", ".join(["%s"] * len(game_data_dict))
                values = tuple(game_data_dict.values())

                # Generate the dynamic INSERT statement
                insert_statement = f"""
                    INSERT INTO team_game_stats ({columns})
                    VALUES ({placeholders})
                """

                # Execute the dynamic INSERT statement
                cursor.execute(insert_statement, values)

        connection.commit()
        # cursor.close()

        # print("Data inserted successfully.")
    except psycopg2.Error as e:
        connection.rollback()

        print("Error inserting data into game_stats table:", e)
