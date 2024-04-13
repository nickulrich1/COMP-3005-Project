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

# Function to process JSON files and insert event data into the database
def process_json_files(folder_path, connection):
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            with open(os.path.join(folder_path, filename), 'r', encoding='utf-8') as file:
                match_id = filename.split('.')[0]  # Extract match ID from filename
                event_data = json.load(file)
                for event in event_data:
                    event_type_name = event.get('type', {}).get('name')
                    standardEvent_data = (
                        event.get('id'),
                        match_id,
                        event.get('index'),
                        event.get('period'),
                        event.get('timestamp'),
                        event.get('minute'),
                        event.get('second'),
                        event.get('type', {}).get('name'),
                        event.get('possession'),
                        event.get('possession_team', {}).get('name'),
                        event.get('play_pattern', {}).get('name'),
                        event.get('team', {}).get('name'),
                        event.get('player', {}).get('id'),
                        event.get('position', {}).get('id'),
                        event.get('location'),
                        event.get('duration'),
                        event.get('under_pressure'),
                        event.get('off_camera'),
                        event.get('out'),
                    )
                    insert_data(connection, 'StandardEvent', standardEvent_data, False)
                    
                    if event_type_name == '50/50':
                        fiftyfifty_data = (
                            event.get('id'),
                            event.get('50/50', {}).get('outcome', {}).get('name'),
                            event.get('50/50', {}).get('counterpress', False)
                        )
                        insert_data(connection, 'FiftyFifty', fiftyfifty_data, False)

                    elif event_type_name == 'Bad Behaviour':
                        behaviour_data = (
                            event.get('id'),
                            event.get('bad_behaviour', {}).get('card', {}).get('id')
                        )
                        insert_data(connection, 'BadBehaviour', behaviour_data, False)
                    
                    elif event_type_name == 'Ball Receipt*':
                        receipt_data = (
                            event.get('id'),
                            event.get('ball_receipt', {}).get('outcome', {}).get('name')
                        )
                        insert_data(connection, 'BallReceipt', receipt_data, False)

                    elif event_type_name == 'Ball Recovery':
                        recovery_data = (
                            event.get('id'),
                            event.get('ball_recovery', {}).get('offensive', False),
                            event.get('ball_recovery', {}).get('recovery_failure', False)
                        )
                        insert_data(connection, 'BallRecovery', recovery_data, False)
                    
                    elif event_type_name == 'Block':
                        block_data = (
                            event.get('id'),
                            event.get('block', {}).get('counterpress', False),
                            event.get('block', {}).get('deflection', False),
                            event.get('block', {}).get('offensive', False),
                            event.get('block', {}).get('save_block', False)
                        )
                        insert_data(connection, 'Block', block_data, False)
                    
                    elif event_type_name == 'Carry':
                        carry_data = (
                            event.get('id'),
                            event.get('carry', {}).get('end_location'),
                        )
                        insert_data(connection, 'Carry', carry_data, False)
                    
                    elif event_type_name == 'Clearance':
                        clearance_data = (
                            event.get('id'),
                            event.get('clearance', {}).get('aerial_won', False),
                            event.get('clearance', {}).get('body_part', {}).get('name')
                        )
                        insert_data(connection, 'Clearance', clearance_data, False)
                    
                    elif event_type_name == 'Dribble':
                        dribble_data = (
                            event.get('id'),
                            event.get('dribble', {}).get('outcome', {}).get('name'),
                            event.get('dribble', {}).get('nutmeg', False),
                            event.get('dribble', {}).get('overrun', False),
                            event.get('dribble', {}).get('no_touch', False),
                        )
                        insert_data(connection, 'Dribble', dribble_data, False)
                    
                    elif event_type_name == 'Dribbled Past':
                        d_past_data = (
                            event.get('id'),
                            event.get('counterpress', False),
                        )
                        insert_data(connection, 'DribbledPast', d_past_data, False)
                                        
                    elif event_type_name == 'Duel':
                        duel_data = (
                            event.get('id'),
                            event.get('duel', {}).get('counterpress', False),
                            event.get('duel', {}).get('type', {}).get('name'),
                            event.get('duel', {}).get('outcome', {}).get('name')
                        )
                        insert_data(connection, 'Duel', duel_data, False)

                    elif event_type_name == 'Foul Committed':
                        foul_data = (
                            event.get('id'),
                            event.get('foul_committed', {}).get('advantage', False),
                            event.get('foul_committed', {}).get('counterpress', False),
                            event.get('foul_committed', {}).get('offensive', False),
                            event.get('foul_committed', {}).get('penalty', False),
                            event.get('foul_committed', {}).get('card', {}).get('id'),
                            event.get('foul_committed', {}).get('type', {}).get('name'),
                        )
                        insert_data(connection, 'FoulCommitted', foul_data, False)

                    elif event_type_name == 'Foul Won':
                        won_data = (
                            event.get('id'),
                            event.get('foul_won', {}).get('advantage', False),
                            event.get('foul_won', {}).get('defensive', False),
                            event.get('foul_won', {}).get('penalty', False),
                        )
                        insert_data(connection, 'FoulWon', won_data, False)
                    
                    elif event_type_name == 'Goal Keeper':
                        keeper_data = (
                            event.get('id'),
                            event.get('goalkeeper', {}).get('position', {}).get('name'),
                            event.get('goalkeeper', {}).get('technique', {}).get('name'),
                            event.get('goalkeeper', {}).get('body_part', {}).get('name'),
                            event.get('goalkeeper', {}).get('type', {}).get('name'),
                            event.get('goalkeeper', {}).get('card', {}).get('outcome'),
                        )
                        insert_data(connection, 'GoalKeeper', keeper_data, False)
                    
                    elif event_type_name == 'Half End':
                        halfend_data = (
                            event.get('id'),
                            event.get('half_end', {}).get('early_video_end', False),
                            event.get('half_end', {}).get('match_suspended', False),
                        )
                        insert_data(connection, 'HalfEnd', halfend_data, False)

                    elif event_type_name == 'Half Start':
                        halfstart_data = (
                            event.get('id'),
                            event.get('half_start', {}).get('late_video_start', False),
                        )
                        insert_data(connection, 'HalfStart', halfstart_data, False)

                    elif event_type_name == 'Injury Stoppage':
                        injurystoppage_data = (
                            event.get('id'),
                            event.get('injury_stoppage', {}).get('in_chain', False),
                        )
                        insert_data(connection, 'InjuryStoppage', injurystoppage_data, False)

                    elif event_type_name == 'Interception':
                        interception_data = (
                            event.get('id'),
                            event.get('interception', {}).get('outcome', {}).get('name'),
                        )
                        insert_data(connection, 'Interception', interception_data, False)
                    
                    elif event_type_name == 'Miscontrol':
                        miscontrol_data = (
                            event.get('id'),
                            event.get('miscontrol', {}).get('aerial_won', False),
                        )
                        insert_data(connection, 'Miscontrol', miscontrol_data, False)
                    
                    elif event_type_name == 'Pass':
                        pass_data = (
                            event.get('id'),
                            event.get('pass', {}).get('recipient', {}).get('name'),
                            event.get('pass', {}).get('length'),
                            event.get('pass', {}).get('angle'),
                            event.get('pass', {}).get('height', {}).get('name'),
                            event.get('pass', {}).get('end_location'),
                            event.get('pass', {}).get('backheel', False),
                            event.get('pass', {}).get('deflected', False),
                            event.get('pass', {}).get('miscommunication', False),
                            event.get('pass', {}).get('cross', False),
                            event.get('pass', {}).get('cut_back', False),
                            event.get('pass', {}).get('switch', False),
                            event.get('pass', {}).get('shot_assist', False),
                            event.get('pass', {}).get('goal_assist', False),
                            event.get('pass', {}).get('body_part', {}).get('name'),
                            event.get('pass', {}).get('type', {}).get('name'),
                            event.get('pass', {}).get('outcome', {}).get('name'),
                            event.get('pass', {}).get('technique', {}).get('name'),
                        )
                        insert_data(connection, 'Pass', pass_data, False)
                    
                    elif event_type_name == 'Player Off':
                        off_data = (
                            event.get('id'),
                            event.get('player off', {}).get('permanent', False),
                        )
                        insert_data(connection, 'PlayerOff', off_data, False)
                    
                    elif event_type_name == 'Pressure':
                        pressure_data = (
                            event.get('id'),
                            event.get('counterpress', False),
                        )
                        insert_data(connection, 'Pressure', pressure_data, False)
                    
                    elif event_type_name == 'Shot':
                        shot_data = (
                            event.get('id'),
                            event.get('shot', {}).get('key_pass_id'),
                            event.get('shot', {}).get('end_location'),
                            event.get('shot', {}).get('aerial_won', False),
                            event.get('shot', {}).get('follows_dribble', False),
                            event.get('shot', {}).get('first_time', False),
                            event.get('shot', {}).get('open_goal', False),
                            event.get('shot', {}).get('statsbomb_xg'),
                            event.get('shot', {}).get('deflected', False),
                            event.get('shot', {}).get('body_part', {}).get('name'),
                            event.get('shot', {}).get('type', {}).get('name'),
                            event.get('shot', {}).get('outcome', {}).get('name'),
                        )
                        insert_data(connection, 'Shot', shot_data, False)
                    
                    elif event_type_name == 'Starting XI':
                        startingxi_data = (
                            event.get('id'),
                            event.get('tactics', {}).get('formation'),
                        )
                        insert_data(connection, 'StartingXI', startingxi_data, False)
                        if (event.get('tactics', {}).get('lineup')):
                            for player in (event.get('tactics', {}).get('lineup')):
                                player_data = (
                                    player.get('jersey_number'),
                                    player.get('player', {}).get('id'),
                                    player.get('position', {}).get('id'),
                                    event.get('id')
                                )
                                insert_data(connection, 'TacticsLineup', player_data, True)
                    
                    elif event_type_name == 'Substitution':
                        substitution_data = (
                            event.get('id'),
                            event.get('substitution', {}).get('replacement', {}).get('id'),
                            event.get('substitution', {}).get('outcome', {}).get('name'),
                        )
                        insert_data(connection, 'Substitution', substitution_data, False)

                    elif event_type_name == 'Tactical Shift':
                        tacticalshift_data = (
                            event.get('id'),
                            event.get('tactics', {}).get('formation'),
                        )
                        insert_data(connection, 'TacticalShift', tacticalshift_data, False)
                        if (event.get('tactics', {}).get('lineup')):
                            for player in (event.get('tactics', {}).get('lineup')):
                                player_data = (
                                    player.get('jersey_number'),
                                    player.get('player', {}).get('id'),
                                    player.get('position', {}).get('id'),
                                    event.get('id')
                                )
                                insert_data(connection, 'TacticsLineup', player_data, True)
                    # Add more conditions for other event types as needed

def main():
    folder_path = "OneDrive - Carleton University/COMP 3005/Final Project/useable-data/events"
    process_json_files(folder_path, connection)
    connection.close()

if __name__ == "__main__":
    main()
