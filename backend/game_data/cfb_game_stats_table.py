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
                home_rushingAttempts INT NULL,
                home_rushingYards INT NULL,
                home_yardsPerPass FLOAT NULL,
                home_completionPercentage INT NULL, 
                home_netPassingYards INT NULL,
                home_totalYards INT NULL,
                home_fourthDownEff INT NULL,
                home_thirdDownEff INT NULL,
                home_firstDowns INT NULL,
                home_rushingTDs INT NULL,
                home_puntReturnYards INT NULL,
                home_puntReturnTDs INT NULL,
                home_puntReturns INT NULL,
                home_passingTDs INT NULL,
                home_kickReturnYards INT NULL,
                home_kickReturnTDs INT NULL,
                home_kickReturns INT NULL,
                home_kickingPoints INT NULL,
                home_interceptionYards INT NULL,
                home_interceptionTDs INT NULL,
                home_passesIntercepted INT NULL,
                home_fumblesRecovered INT NULL,
                home_totalFumbles FLOAT NULL,
                home_tacklesForLoss FLOAT NULL,
                home_defensiveTDs INT NULL,
                home_tackles INT NULL,
                home_sacks INT NULL,
                home_qbHurries INT NULL,
                home_passesDeflected INT NULL,
                home_possessionTime INT NULL,
                home_interceptions INT NULL,
                home_fumblesLost INT NULL,
                home_turnovers INT NULL,
                home_totalPenaltiesYards INT NULL,
                home_yardsPerRushAttempt FLOAT NULL,
                away_school_id INT,
                away_home_away VARCHAR,
                away_points INT,
                away_rushingAttempts INT NULL,
                away_rushingYards INT NULL,
                away_yardsPerPass FLOAT NULL,
                away_completionPercentage INT NULL, 
                away_netPassingYards INT NULL,
                away_totalYards INT NULL,
                away_fourthDownEff INT NULL,
                away_thirdDownEff INT NULL,
                away_firstDowns INT NULL,
                away_rushingTDs INT NULL,
                away_puntReturnYards INT NULL,
                away_puntReturnTDs INT NULL,
                away_puntReturns INT NULL,
                away_passingTDs INT NULL,
                away_kickReturnYards INT NULL,
                away_kickReturnTDs INT NULL,
                away_kickReturns INT NULL,
                away_kickingPoints INT NULL,
                away_interceptionYards INT NULL,
                away_interceptionTDs INT NULL,
                away_passesIntercepted INT NULL,
                away_fumblesRecovered INT NULL,
                away_totalFumbles FLOAT NULL,
                away_tacklesForLoss FLOAT NULL,
                away_defensiveTDs INT NULL,
                away_tackles INT NULL,
                away_sacks INT NULL,
                away_qbHurries INT NULL,
                away_passesDeflected INT NULL,
                away_possessionTime INT NULL,
                away_interceptions INT NULL,
                away_fumblesLost INT NULL,
                away_turnovers INT NULL,
                away_totalPenaltiesYards INT NULL,
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
