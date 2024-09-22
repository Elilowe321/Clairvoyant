##############################
# Initial test to make sure  #
# docker database is working #
##############################

import psycopg2
import os
from dotenv import load_dotenv
from database.database_commands import create_connection
from game_data import cfb_teams_table, cfb_games_table, cfb_team_talent_table, cfb_recruiting_table, cfb_betting_lines_table


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
    for year in range (1900, 2025):
        cfb_recruiting_table.get_team_recruiting(connection, year)

    # Call every week to get the betting lines for a football game
    for year in range (1900, 2025):
        for week in range (1, 15):
            cfb_betting_lines_table.get_betting_lines(connection, year, week)




    connection.close()