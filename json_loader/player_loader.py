import json
import os
import psycopg2
import sys

connection = psycopg2.connect(
            dbname="final_project",
            user="postgres",
            password="7pooDle88",
            host="localhost",
            port="5432"
        )


# Function to insert data into the specified table
def insert_data(connection, table_name, data, query):
    try:
        cursor = connection.cursor()
        num_values = len(data)
        placeholders = ','.join(['%s'] * num_values)
        if query:
            sql_query = f"INSERT INTO {table_name} VALUES (DEFAULT, {placeholders})"
        else: 
            sql_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.execute(sql_query, data)
        connection.commit()
        cursor.close()
        print(f"Data inserted into {table_name} successfully.")
    except psycopg2.Error as e:
        connection.rollback()
        print(f"Error inserting data into {table_name}: {e}")

def insert_data_in_player_table(connection, player_data, team_id, match_id):
    
    player2_data = (player_data.get('player_id'), player_data.get('player_name'), player_data.get('player_nickname'), player_data.get('jersey_number'), player_data.get('country', {}).get('name'))
    insert_data(connection, "Player", player2_data, False)
    
    matchplayer_data = (team_id, match_id, player_data.get('player_id'))
    insert_data(connection, "MatchPlayers", matchplayer_data, False)
   
    for position in player_data.get('positions', {}):
        gameposition_data = (position.get('position_id'), position.get('from'), position.get('to'), position.get('from_period'), position.get('to_period'), position.get('start_reason'), position.get('end_reason'), team_id, match_id, player_data.get('player_id'))
        insert_data(connection, "GamePosition", gameposition_data, True)
        position_data = (position.get('position_id'), position.get('position'))
        insert_data(connection, "Position", position_data, False)

    for card in player_data.get('cards', {}):
        card_data = (card.get('time'), card.get('card_type'), card.get('reason'), card.get('period'), team_id, match_id, player_data.get('player_id'))
        insert_data(connection, "Card", card_data, True)
    

def process_json_files(folder_path):
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            match_id = int(os.path.splitext(filename)[0])
            with open(os.path.join(folder_path, filename), 'r', encoding='utf-8') as file:  
                lineup_data = json.load(file)
                for item in lineup_data:
                    item_data = item.get('team_id')
                    for player in item['lineup']:
                        insert_data_in_player_table(connection, player, item_data, match_id)
folder_path = "OneDrive - Carleton University/COMP 3005/Final Project/useable-data/lineups"

process_json_files(folder_path)

connection.close()
