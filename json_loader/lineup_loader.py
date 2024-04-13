import json
import os
import psycopg2
connection = psycopg2.connect(
    dbname="final_project",
    user="postgres",
    password="7pooDle88",
    host="localhost",
    port="5432"
)

cursor = connection.cursor()
data_folder = "OneDrive - Carleton University/COMP 3005/Final Project/useable-data/matches/"

for subdir, dirs, files in os.walk(data_folder):
    for file in files:
        file_path = os.path.join(subdir, file)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as json_file:
                matches = json.load(json_file)
        except UnicodeDecodeError as e:
            print(f"Error reading file {file_path}:")
            continue
        
        for match in matches:
           
            match_id = match.get('match_id')
            home_team_id = match.get('home_team', {}).get('home_team_id')
            away_team_id = match.get('away_team', {}).get('away_team_id')
            
           
            if match_id is not None and home_team_id is not None and away_team_id is not None:
                cursor.execute("""
                    INSERT INTO Lineup (match_id, home_team_id, away_team_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (match_id) DO NOTHING;
                """, (match_id, home_team_id, away_team_id))
        
        
        connection.commit()
        print(f"Data committed for file: {file_path}")


cursor.close()
connection.close()
