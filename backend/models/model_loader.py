import pandas as pd
from models.create_models import (
    gradient_boost,
    random_forest,
    user_created_model,
    target_provided
)

def model_loader(
    connection, given_columns, user_id, name, type, target, description=None
):

    # Get all game stats through sql database command
    rows = get_column_stats(connection, given_columns)

    # Convert to DataFrame and sort
    df = pd.DataFrame(rows, columns=given_columns)
    df = df.sort_index(axis=1)

    # Call different models to compare which is the best
    model = target_provided(
        user_id=user_id,
        name=name,
        df=df,
        type=type,
        target=target,
        description=description,
    )

    return model



# Returns the stats for given columns
def get_column_stats(connection, columns):
    try:
        # Need to remove for select query
        removed = False
        if "target" in columns:
            columns.remove("target")
            removed = True

        # Construct the list of columns for SQL query
        columns_str = ", ".join(columns)
        cursor = connection.cursor()

        if removed:
            # Execute the SQL query with the formatted columns string
            cursor.execute(
                f"""
                SELECT {columns_str},
                    CASE
                        WHEN home_points > away_points THEN 1
                        ELSE 0
                    END AS target
                FROM (SELECT 
                    tgs.*,
                    home_team.talent AS home_talent,
                    away_team.talent AS away_talent
                FROM 
                    team_game_stats tgs
                JOIN (
                    SELECT 
                        t.id, 
                        tt.talent, 
                        tt.year
                    FROM 
                        team_talent tt
                    JOIN 
                        teams t ON t.school = tt.school
                ) home_team ON (home_team.id = tgs.home_school_id) AND (home_team.year = tgs.year)
                JOIN (
                    SELECT 
                        t.id, 
                        tt.talent, 
                        tt.year
                    FROM 
                        team_talent tt
                    JOIN 
                        teams t ON t.school = tt.school
                ) away_team ON (away_team.id = tgs.away_school_id) AND (away_team.year = tgs.year));
                """
            )

        else:
            cursor.execute(
                f"""
                SELECT {columns_str}
                    FROM (SELECT 
                        tgs.*,
                        home_team.talent AS home_talent,
                        away_team.talent AS away_talent
                    FROM 
                        team_game_stats tgs
                    JOIN (
                        SELECT 
                            t.id, 
                            tt.talent, 
                            tt.year
                        FROM 
                            team_talent tt
                        JOIN 
                            teams t ON t.school = tt.school
                    ) home_team ON (home_team.id = tgs.home_school_id) AND (home_team.year = tgs.year)
                    JOIN (
                        SELECT 
                            t.id, 
                            tt.talent, 
                            tt.year
                        FROM 
                            team_talent tt
                        JOIN 
                            teams t ON t.school = tt.school
                    ) away_team ON (away_team.id = tgs.away_school_id) AND (away_team.year = tgs.year));
                """
            )

        # Fetch all rows
        rows = cursor.fetchall()
        cursor.close()

        # Add back removed column
        if removed:
            columns.append("target")

        return rows

    except Exception as e:
        print(f"Error executing SQL query: {e}")
        return None