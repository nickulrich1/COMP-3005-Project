import os
import json
import psycopg2

connection = psycopg2.connect(
            dbname="final_project",
            user="postgres",
            password="7pooDle88",
            host="localhost",
            port="5432"
        )
def insert_data_in_season_table(connection, season_data):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO Season (id, name)
            VALUES (%s, %s);
        """, (season_data.get('season_id'), season_data.get('season_name')))
        connection.commit()
        cursor.close()
        print("Season data inserted successfully.")
    except psycopg2.IntegrityError as e:
        connection.rollback()  
        print("Duplicate season data.", e)
    except psycopg2.Error as e:
        print("Error inserting season data:", e)

def insert_data_in_competition_table(connection, competition_data):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO Competition (competition_id, season_id, competition_name, competition_gender,
                                     country_name, season_name)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (competition_data.get('competition_id'), competition_data.get('season_id'),
              competition_data.get('competition_name'), competition_data.get('competition_gender'),
              competition_data.get('country_name'), competition_data.get('season_name')))
        connection.commit()
        cursor.close()
        print("Competition data inserted successfully.")
    except psycopg2.IntegrityError as e:
        connection.rollback()  
        print("Duplicate competition data. Skipping insertion:", e)
    except psycopg2.Error as e:
        print("Error inserting competition data:", e)

with open('OneDrive - Carleton University/COMP 3005/Final Project/open-data/data/competitions.json', 'r') as file:
    competitions_data = json.load(file)

for competition in competitions_data:
    season_data = {
        'season_id': competition['season_id'],
        'season_name': competition['season_name']
    }
    insert_data_in_season_table(connection, season_data)
    insert_data_in_competition_table(connection, competition)
connection.close()
