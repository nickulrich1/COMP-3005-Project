# Created by Gabriel Martell

'''
Version 1.2 (04/13/2024)
=========================================================
queries.py (Carleton University COMP3005 - Database Management Student Template Code)

This is the template code for the COMP3005 Database Project 1, and must be accomplished on an Ubuntu Linux environment.
Your task is to ONLY write your SQL queries within the prompted space within each Q_# method (where # is the question number).

You may modify code in terms of testing purposes (commenting out a Qn method), however, any alterations to the code, such as modifying the time, 
will be flagged for suspicion of cheating - and thus will be reviewed by the staff and, if need be, the Dean. 

To review the Integrity Violation Attributes of Carleton University, please view https://carleton.ca/registrar/academic-integrity/ 

=========================================================
'''

# Imports
import psycopg
import csv
import subprocess
import os
import re

# Connection Information
''' 
The following is the connection information for this project. These settings are used to connect this file to the autograder.
You must NOT change these settings - by default, db_host, db_port and db_username are as follows when first installing and utilizing psql.
For the user "postgres", you must MANUALLY set the password to 1234.

This can be done with the following snippet:

sudo -u postgres psql
\password postgres

'''
root_database_name = "project_database"
query_database_name = "query_database"
db_username = 'postgres'
db_password = '1234'
db_host = 'localhost'
db_port = '5432'

# Directory Path - Do NOT Modify
dir_path = os.path.dirname(os.path.realpath(__file__))

# Loading the Database after Drop - Do NOT Modify
#================================================
def load_database(conn):
    drop_database(conn)

    cursor = conn.cursor()
    # Create the Database if it DNE
    try:
        conn.autocommit = True
        cursor.execute(f"CREATE DATABASE {query_database_name};")
        conn.commit()

    except Exception as error:
        print(error)

    finally:
        cursor.close()
        conn.autocommit = False
    conn.close()
    
    # Connect to this query database.
    dbname = query_database_name
    user = db_username
    password = db_password
    host = db_host
    port = db_port
    conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)

    # Import the dbexport.sql database data into this database
    try:
        command = f'psql -h {host} -U {user} -d {query_database_name} -a -f "{os.path.join(dir_path, "dbexport.sql")}" > /dev/null 2>&1'
        env = {'PGPASSWORD': password}
        subprocess.run(command, shell=True, check=True, env=env)

    except Exception as error:
        print(f"An error occurred while loading the database: {error}")
    
    # Return this connection.
    return conn    

# Dropping the Database after Query n Execution - Do NOT Modify
#================================================
def drop_database(conn):
    # Drop database if it exists.

    cursor = conn.cursor()

    try:
        conn.autocommit = True
        cursor.execute(f"DROP DATABASE IF EXISTS {query_database_name};")
        conn.commit()

    except Exception as error:
        print(error)
        pass

    finally:
        cursor.close()
        conn.autocommit = False

# Reconnect to Root Database - Do NOT Modify
#================================================
def reconnect():
    dbname = root_database_name
    user = db_username
    password = db_password
    host = db_host
    port = db_port
    return psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)

# Getting the execution time of the query through EXPLAIN ANALYZE - Do NOT Modify
#================================================
def get_time(cursor, sql_query):
    # Prefix your query with EXPLAIN ANALYZE
    explain_query = f"EXPLAIN ANALYZE {sql_query}"

    try:
        # Execute the EXPLAIN ANALYZE query
        cursor.execute(explain_query)
        
        # Fetch all rows from the cursor
        explain_output = cursor.fetchall()
        
        # Convert the output tuples to a single string
        explain_text = "\n".join([row[0] for row in explain_output])
        
        # Use regular expression to find the execution time
        # Look for the pattern "Execution Time: <time> ms"
        match = re.search(r"Execution Time: ([\d.]+) ms", explain_text)
        if match:
            execution_time = float(match.group(1))
            return f"Execution Time: {execution_time} ms"
        else:
            print("Execution Time not found in EXPLAIN ANALYZE output.")
            return f"NA"
        
    except Exception as error:
        print(f"[ERROR] Error getting time.\n{error}")


# Write the results into some Q_n CSV. If the is an error with the query, it is a INC result - Do NOT Modify
#================================================
def write_csv(execution_time, cursor, i):
    # Collect all data into this csv, if there is an error from the query execution, the resulting time is INC.
    try:
        colnames = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        filename = f"{dir_path}/Q_{i}.csv"

        with open(filename, 'w', encoding='utf-8', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            
            # Write column names to the CSV file
            csvwriter.writerow(colnames)
            
            # Write data rows to the CSV file
            csvwriter.writerows(rows)

    except Exception as error:
        execution_time[i-1] = "INC"
        print(error)
    
#================================================
        
'''
The following 10 methods, (Q_n(), where 1 < n < 10) will be where you are tasked to input your queries.
To reiterate, any modification outside of the query line will be flagged, and then marked as potential cheating.
Once you run this script, these 10 methods will run and print the times in order from top to bottom, Q1 to Q10 in the terminal window.
'''
def Q_1(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT player_name, AVG(Shot.statsbomb_xg) AS AvgXG
FROM Player 
INNER JOIN StandardEvent ON Player.player_id = StandardEvent.player_id 
INNER JOIN Match ON Match.match_id = StandardEvent.match_id
INNER JOIN Season ON Season.id = Match.season_id
INNER JOIN Competition ON Competition.competition_id = Match.competition_id AND Competition.season_id = Match.season_id
INNER JOIN Shot ON Shot.event_id = StandardEvent.id 
WHERE Competition.competition_name = 'La Liga' AND Season.name = '2020/2021' 
GROUP BY player_name
ORDER BY AVG(Shot.statsbomb_xg) DESC"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[0] = (time_val)

    write_csv(execution_time, cursor, 1)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_2(conn, execution_time):

    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT player_name, COUNT (player_name) AS num_shots
FROM Player
INNER JOIN StandardEvent ON Player.player_id = StandardEvent.player_id 
INNER JOIN Match ON Match.match_id = StandardEvent.match_id
INNER JOIN Season ON Season.id = Match.season_id
INNER JOIN Competition ON Competition.competition_id = Match.competition_id AND Competition.season_id = Match.season_id
INNER JOIN Shot ON Shot.event_id = StandardEvent.id
WHERE Competition.competition_name = 'La Liga' AND Season.name = '2020/2021'
GROUP BY player_name 
ORDER BY COUNT (player_name) DESC"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[1] = (time_val)

    write_csv(execution_time, cursor, 2)

    cursor.close()
    new_conn.close()

    return reconnect()
    
def Q_3(conn, execution_time):

    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT player_name, COUNT (player_name) AS num_first_time
FROM Player
INNER JOIN StandardEvent ON Player.player_id = StandardEvent.player_id 
INNER JOIN Match ON Match.match_id = StandardEvent.match_id
INNER JOIN Season ON Season.id = Match.season_id
INNER JOIN Competition ON Competition.competition_id = Match.competition_id AND Competition.season_id = Match.season_id
INNER JOIN Shot ON Shot.event_id = StandardEvent.id
WHERE Shot.first_time = True AND Competition.competition_name = 'La Liga' AND (Season.name = '2018/2019' OR Season.name = '2019/2020' OR Season.name = '2020/2021')
GROUP BY player_name
ORDER BY COUNT(player_name) DESC"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[2] = (time_val)

    write_csv(execution_time, cursor, 3)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_4(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT team_name, COUNT (team_name) AS num_passes
FROM StandardEvent
INNER JOIN Pass ON StandardEvent.id = Pass.event_id
INNER JOIN Match ON Match.match_id = StandardEvent.match_id
INNER JOIN Season ON Season.id = Match.season_id
INNER JOIN Competition ON Competition.competition_id = Match.competition_id AND Competition.season_id = Match.season_id
WHERE Competition.competition_name = 'La Liga' AND Season.name = '2020/2021' 
GROUP BY team_name 
ORDER BY COUNT (team_name) DESC"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[3] = (time_val)

    write_csv(execution_time, cursor, 4)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_5(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """WITH players_num(recipient_name,num_recived_passes) AS
(
SELECT recipient_name, COUNT(recipient_name) AS received_Count
FROM Pass JOIN StandardEvent ON Pass.event_id = StandardEvent.id JOIN Match m1 ON StandardEvent.match_id = m1.match_id JOIN Competition ON m1.competition_id = Competition.competition_id AND m1.season_id = Competition.season_id
WHERE Competition.competition_name = 'Premier League' AND Competition.season_name = '2003/2004'
GROUP BY recipient_name
ORDER BY received_Count DESC
)
SELECT recipient_name,num_recived_passes
FROM players_num
WHERE num_recived_passes > 0;"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[4] = (time_val)

    write_csv(execution_time, cursor, 5)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_6(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """WITH shots_num(team_name,num_shots) AS
(
SELECT team_name, COUNT(team_name) AS shots_Count
FROM Shot JOIN StandardEvent ON Shot.event_id = StandardEvent.id JOIN Match m1 ON StandardEvent.match_id = m1.match_id JOIN Competition ON m1.competition_id = Competition.competition_id AND m1.season_id = Competition.season_id
WHERE Competition.competition_name = 'Premier League' AND Competition.season_name = '2003/2004'
GROUP BY team_name
ORDER BY shots_Count DESC
)
SELECT team_name,num_shots
FROM shots_num
WHERE num_shots > 0;"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[5] = (time_val)

    write_csv(execution_time, cursor, 6)

    cursor.close()
    new_conn.close()

    return reconnect()


def Q_7(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """WITH pass_num(player_name,num_through_balls) AS
(
SELECT player_name, COUNT(player_name) AS pass_Count
FROM Pass JOIN StandardEvent ON Pass.event_id = StandardEvent.id JOIN Player ON StandardEvent.player_id = Player.player_id JOIN Match m1 ON StandardEvent.match_id = m1.match_id JOIN Competition ON m1.competition_id = Competition.competition_id AND m1.season_id = Competition.season_id
WHERE Competition.competition_name = 'La Liga' AND Competition.season_name = '2020/2021' AND Pass.technique_name='Through Ball'
GROUP BY player_name
ORDER BY pass_Count DESC
)
SELECT player_name,num_through_balls
FROM pass_num
WHERE num_through_balls > 0;"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[6] = (time_val)

    write_csv(execution_time, cursor, 7)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_8(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT 
    t.name AS team_name,
    COUNT(p.event_id) AS through_ball_count
FROM
    Pass p
JOIN
    StandardEvent se ON p.event_id = se.id
JOIN
    Match m ON se.match_id = m.match_id
JOIN
    Competition c ON m.competition_id = c.competition_id
        AND m.season_id = c.season_id
JOIN
    Team t ON se.team_name = t.name
WHERE
    c.competition_name = 'La Liga'
    AND c.season_name = '2020/2021'
    AND p.technique_name = 'Through Ball'
GROUP BY
    t.name
ORDER BY
    through_ball_count DESC;"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[7] = (time_val)

    write_csv(execution_time, cursor, 8)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_9(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT p.player_name AS player_name, COUNT(d.event_id) AS completed_dribbles
FROM Dribble d
    JOIN StandardEvent sevent ON d.event_id = sevent.id
    JOIN Player p ON sevent.player_id = p.player_id
    JOIN Match m ON sevent.match_id = m.match_id
    JOIN Competition comp ON m.competition_id = comp.competition_id AND m.season_id = comp.season_id
WHERE
    comp.competition_name = 'La Liga' AND
    comp.season_name IN ('2020/2021', '2019/2020', '2018/2019') AND
    d.outcome_name = 'Complete'
GROUP BY
    p.player_name
ORDER BY
    completed_dribbles DESC;"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[8] = (time_val)

    write_csv(execution_time, cursor, 9)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_10(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT player_name, COUNT(player_name) AS dribbled_count 
FROM Player
INNER JOIN StandardEvent ON Player.player_id = StandardEvent.player_id
INNER JOIN Match ON Match.match_id = StandardEvent.match_id
INNER JOIN Season ON Season.id = Match.season_id
INNER JOIN Competition ON Competition.competition_id = Match.competition_id AND Competition.season_id = Match.season_id
INNER JOIN DribbledPast ON DribbledPast.event_id = StandardEvent.id
WHERE Competition.competition_name = 'La Liga' AND Season.name = '2020/2021'
GROUP BY player_name
ORDER BY COUNT (player_name) ASC"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[9] = (time_val)

    write_csv(execution_time, cursor, 10)

    cursor.close()
    new_conn.close()

    return reconnect()

# Running the queries from the Q_n methods - Do NOT Modify
#=====================================================
def run_queries(conn):

    execution_time = [0,0,0,0,0,0,0,0,0,0]

    conn = Q_1(conn, execution_time)
    conn = Q_2(conn, execution_time)
    conn = Q_3(conn, execution_time)
    conn = Q_4(conn, execution_time)
    conn = Q_5(conn, execution_time)
    conn = Q_6(conn, execution_time)
    conn = Q_7(conn, execution_time)
    conn = Q_8(conn, execution_time)
    conn = Q_9(conn, execution_time)
    conn = Q_10(conn, execution_time)

    for i in range(10):
        print(execution_time[i])

''' MAIN '''
try:
    if __name__ == "__main__":

        dbname = root_database_name
        user = db_username
        password = db_password
        host = db_host
        port = db_port

        conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        
        run_queries(conn)
except Exception as error:
    print(error)
    #print("[ERROR]: Failure to connect to database.")
#_______________________________________________________
