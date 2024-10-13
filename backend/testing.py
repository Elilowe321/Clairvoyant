##############################
# Initial test to make sure  #
# docker database is working #
##############################

import psycopg2
import os
from dotenv import load_dotenv
from database.database_commands import create_connection
from game_data import cfb_teams_table, cfb_games_table, cfb_team_talent_table, cfb_recruiting_table, cfb_betting_lines_table, cfb_game_stats_table
from models import model_loader, predict_games
from global_vars import Global
from pprint import pprint



# All columns in the database
def model_columns():
    return [
        'away_completionpercentage', 'away_defensivetds', 'away_firstdowns', 'away_fourthdowneff', 
        'away_fumbleslost', 'away_fumblesrecovered', 'away_interceptions', 'away_interceptiontds', 
        'away_interceptionyards', 'away_kickingpoints', 'away_kickreturns', 'away_kickreturntds', 
        'away_kickreturnyards', 'away_netpassingyards', 'away_passesdeflected', 'away_passesintercepted', 
        'away_passingtds', 'away_points', 'away_possessiontime', 'away_puntreturns', 'away_puntreturntds', 
        'away_puntreturnyards', 'away_qbhurries', 'away_rushingattempts', 'away_rushingtds', 'away_rushingyards', 
        'away_sacks', 'away_tackles', 'away_tacklesforloss', 'away_talent', 'away_thirddowneff', 
        'away_totalfumbles', 'away_totalpenaltiesyards', 'away_totalyards', 'away_turnovers', 'away_yardsperpass', 
        'away_yardsperrushattempt', 'home_completionpercentage', 'home_defensivetds', 'home_firstdowns', 
        'home_fourthdowneff', 'home_fumbleslost', 'home_fumblesrecovered', 'home_interceptions', 
        'home_interceptiontds', 'home_interceptionyards', 'home_kickingpoints', 'home_kickreturns', 
        'home_kickreturntds', 'home_kickreturnyards', 'home_netpassingyards', 'home_passesdeflected', 
        'home_passesintercepted', 'home_passingtds', 'home_points', 'home_possessiontime', 'home_puntreturns', 
        'home_puntreturntds', 'home_puntreturnyards', 'home_qbhurries', 'home_rushingattempts', 'home_rushingtds', 
        'home_rushingyards', 'home_sacks', 'home_tackles', 'home_tacklesforloss', 'home_talent', 
        'home_thirddowneff', 'home_totalfumbles', 'home_totalpenaltiesyards', 'home_totalyards', 
        'home_turnovers', 'home_yardsperpass', 'home_yardsperrushattempt', 'target'
    ]


# load env variables
load_dotenv()

connection = create_connection()

if connection:

    # Only need to call once to get all teams
    # cfb_teams_table.get_teams(connection)

    # Call once a year to get all games for a year
    # for year in range (1900, 2025):
    #     cfb_games_table.get_games(connection, year)

    # Call once a year to get team talent for every team
    # for year in range (2015, 2025):
    #     cfb_team_talent_table.get_team_talent(connection, year)

    # Call once a year to get recruiting stats for each team
    # for year in range (2000, 2025):
    #     cfb_recruiting_table.get_team_recruiting(connection, year)

    # Call every week to get the betting lines for a football game
    # for year in range (2013, 2025):
        # for week in range (1, 15):
        #     cfb_betting_lines_table.get_betting_lines(connection, year, week)

    # cfb_betting_lines_table.get_betting_lines(connection, Global.year, Global.week)

    # Call every week to get the game stats for a football game
    # for year in range (2001, 2025):
        # for week in range (1, 15):
            # cfb_game_stats_table.get_game_stats(connection, year, week)

    # cfb_game_stats_table.get_game_stats(connection, Global.year, Global.week)



    # Creating models
    user_model = model_loader.model_loader(connection, model_columns(), 1, 'test', 'classification', 'target', 'desc')
    # pprint(user_model)
    games = predict_games.predict_games(connection, Global.year, Global.week, 'classification', 'target', model_columns(), 'cfb_models/1_class_test.joblib', None)
    # Iterate over the games dictionary
    for game_id, game_data in games.items():
        if game_data['home_team'] == 'Florida':
            pprint(game_data)


    connection.close()




    