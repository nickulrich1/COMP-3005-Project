import json
import os  # Import the os module
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
                print(f"Processing file: {file_path}")
        except UnicodeDecodeError as e:
            print(f"Error reading file {file_path}: {e}")
            continue
        
        
        for match in matches:
            
            home_managers = match.get('home_team', {}).get('managers', [])
            for manager in home_managers:
                manager_id = manager.get('id')
                name = manager.get('name')
                nickname = manager.get('nickname')
                dob = manager.get('dob')
                country_name = manager.get('country', {}).get('name')
                
                if manager_id is not None and name is not None and dob is not None and country_name is not None:
                    
                    cursor.execute("""
                        INSERT INTO Manager (id, name, nickname, dob, country_name)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                    """, (manager_id, name, nickname, dob, country_name))
                    print(f"Inserted manager: {name}")

            
            away_managers = match.get('away_team', {}).get('managers', [])
            for manager in away_managers:
                manager_id = manager.get('id')
                name = manager.get('name')
                nickname = manager.get('nickname')
                dob = manager.get('dob')
                country_name = manager.get('country', {}).get('name')
                
                if manager_id is not None and name is not None and dob is not None and country_name is not None:
                    
                    cursor.execute("""
                        INSERT INTO Manager (id, name, nickname, dob, country_name)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                    """, (manager_id, name, nickname, dob, country_name))
                    print(f"Inserted manager: {name}")
        
        
        connection.commit()
        


cursor.close()
connection.close()
print("Database connection closed.")
