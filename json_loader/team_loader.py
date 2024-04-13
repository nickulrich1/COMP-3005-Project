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

records_inserted = 0

for subdir, dirs, files in os.walk(data_folder):
    for file in files:
        file_path = os.path.join(subdir, file)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as json_file:
                matches = json.load(json_file)
        except UnicodeDecodeError as e:
            print(f"Error reading file {file_path}: {e}")
            continue
        

        for match in matches:
 
            home_team = match.get('home_team', {})
            home_team_id = home_team.get('home_team_id')
            home_team_name = home_team.get('home_team_name')
            home_team_gender = home_team.get('home_team_gender')
            home_team_group = home_team.get('home_team_group')
            home_team_country = home_team.get('country', {}).get('name')
            
 
            home_team_managers = home_team.get('managers', [])
            home_manager_id = None
            if home_team_managers:
                home_manager_id = home_team_managers[0].get('id')

            
            if home_team_id and home_team_name and home_team_country:
                try:
                    
                    cursor.execute("""
                        INSERT INTO Team (id, name, gender, team_group, country_name)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                    """, (home_team_id, home_team_name, home_team_gender, home_team_group, home_team_country))

                    
                    records_inserted += 1
                except Exception as e:
                    print(f"Error inserting home team data (ID: {home_team_id})")

            
            away_team = match.get('away_team', {})
            away_team_id = away_team.get('away_team_id')
            away_team_name = away_team.get('away_team_name')
            away_team_gender = away_team.get('away_team_gender')
            away_team_group = away_team.get('away_team_group')
            away_team_country = away_team.get('country', {}).get('name')
            
            
            away_team_managers = away_team.get('managers', [])
            away_manager_id = None
            if away_team_managers:
                away_manager_id = away_team_managers[0].get('id')

            
            if away_team_id and away_team_name and away_team_country:
                try:
                   
                    cursor.execute("""
                        INSERT INTO Team (id, name, gender, team_group, country_name)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                    """, (away_team_id, away_team_name, away_team_gender, away_team_group, away_team_country))

                   
                    records_inserted += 1
                except Exception as e:
                    print(f"Error inserting away team data (ID: {away_team_id})", e)
        
        
        connection.commit()
        print(f"Data committed for file: {file_path}")


cursor.close()
connection.close()

print(f"Total records inserted successfully: {records_inserted}")
