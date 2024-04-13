import os
import json
import psycopg2
from datetime import datetime

# Database connection details
connection = psycopg2.connect(
    dbname="final_project",
    user="postgres",
    password="7pooDle88",
    host="localhost",
    port="5432"
)

base_directory = "OneDrive - Carleton University/COMP 3005/Final Project/useable-data/matches/"

def insert_match_data(cursor, match):
    # SQL query to insert data into the Match table
    insert_query = """
        INSERT INTO Match (
            match_id,
            competition_id,
            country_name,
            season_id,
            match_date,
            kick_off,
            stadium_name,
            stadium_country,
            referee_name,
            referee_country_name,
            home_team_manager_id,
            away_team_manager_id,
            home_score,
            away_score,
            match_week,
            competition_stage_name
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (match_id) DO NOTHING;
    """

    match_date = datetime.strptime(match['match_date'], '%Y-%m-%d').date()
    kick_off = datetime.strptime(match['kick_off'], '%H:%M:%S.%f').time() if match['kick_off'] else None

    referee = match.get('referee', {})
    referee_name = referee.get('name', None)
    referee_country_name = referee.get('country', {}).get('name', None)

    # Get stadium details
    stadium = match.get('stadium', {})
    stadium_name = stadium.get('name', None)
    stadium_country = stadium.get('country', {}).get('name', None)

    # Get manager IDs for home and away teams
    home_manager_id = None
    away_manager_id = None
    
    home_team_managers = match.get('home_team', {}).get('managers', [])
    if home_team_managers:
        home_manager_id = home_team_managers[0].get('id', None)

    away_team_managers = match.get('away_team', {}).get('managers', [])
    if away_team_managers:
        away_manager_id = away_team_managers[0].get('id', None)

    cursor.execute(
        insert_query,
        (
            match.get('match_id'),
            match['competition']['competition_id'],
            match['competition']['country_name'],
            match['season']['season_id'],
            match_date,
            kick_off,
            stadium_name,
            stadium_country,
            referee_name,
            referee_country_name,
            home_manager_id,
            away_manager_id,
            match['home_score'],
            match['away_score'],
            match['match_week'],
            match.get('competition_stage', {}).get('name', None)
        )
    )
    
    rows_affected = cursor.rowcount
    if rows_affected == 0:
        print(f"Duplicate match data found: Match ID={match['match_id']}")
    else:
        print(f"Inserted match: Match ID={match['match_id']}")
def process_json_files(cursor, folder_path):
    for root, _, files in os.walk(folder_path):
        for file_name in files:
            if file_name.endswith('.json'):
                file_path = os.path.join(root, file_name)
                print(f"Reading data from file: {file_name}")
                with open(file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                for match in data:
                    insert_match_data(cursor, match)
with connection:
    cursor = connection.cursor()
    process_json_files(cursor, base_directory)
    connection.commit()
    cursor.close()
