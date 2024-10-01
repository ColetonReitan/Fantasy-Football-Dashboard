## To update the data within the database, follow the code below.


### Connecting to sql database
```python
import mysql.connector

# Connect to MySQL
db_connection = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="****",
    database="Fantasy_Dash2"
)
```

### Cleaning and Merging FantasyPros CSVs    
This also gives sleeper player id's to the fantasypros players  
Need to change the file paths as well as week in this code in order to stay current. 
```python
import pandas as pd
import mysql.connector
import os
import re

# Create a cursor object
db_cursor = db_connection.cursor(dictionary=True)

# List of CSV file paths for each position
csv_files = {
    'DEF': r"C:\Users\colet\OneDrive\Documents\FantasyDashboardProject\FantasyPros_Fantasy_Football_Statistics_DST_week4.csv",
    'QB': r"C:\Users\colet\OneDrive\Documents\FantasyDashboardProject\FantasyPros_Fantasy_Football_Statistics_QB_week4.csv",
    'RB': r"C:\Users\colet\OneDrive\Documents\FantasyDashboardProject\FantasyPros_Fantasy_Football_Statistics_RB_week4.csv",
    'WR': r"C:\Users\colet\OneDrive\Documents\FantasyDashboardProject\FantasyPros_Fantasy_Football_Statistics_WR_week4.csv",
    'TE': r"C:\Users\colet\OneDrive\Documents\FantasyDashboardProject\FantasyPros_Fantasy_Football_Statistics_TE_week4.csv",
    'K': r"C:\Users\colet\OneDrive\Documents\FantasyDashboardProject\FantasyPros_Fantasy_Football_Statistics_K_week4.csv"
}

# Fetch players from the SQL database
query = "SELECT player_id, LOWER(first_name) AS first_name, LOWER(last_name) AS last_name, team AS team, position FROM Players"
db_cursor.execute(query)
players_from_db = pd.DataFrame(db_cursor.fetchall())

# Remove punctuation and spaces from the first_name and last_name in SQL data
players_from_db['first_name'] = players_from_db['first_name'].apply(lambda x: re.sub(r'[^\w\s]', '', str(x)))
players_from_db['last_name'] = players_from_db['last_name'].apply(lambda x: re.sub(r'[^\w\s]', '', str(x)).replace(" ", ""))

# Normalize the position column in the database
players_from_db['position'] = players_from_db['position'].str.upper()

# Define a mapping of SQL positions to CSV positions
position_mapping = {
    'RB': ['RB', 'FB'],  # Running Back includes Fullback in SQL
    'QB': ['QB'],
    'WR': ['WR'],
    'TE': ['TE'],
    'K': ['K'],
    'DEF': ['DEF']
}

# Function to split the player field and extract first and last names
def split_player_name(player_field):
    # Remove the team acronym (e.g., "(BUF)")
    player_name = player_field.split(" (")[0]
    name_parts = player_name.split()

    # Assume everything before the first space is the first name and the rest is the last name
    first_name = name_parts[0]
    last_name = " ".join(name_parts[1:])  # Keep the last name as is initially

    return first_name, last_name

# Function to remove punctuation from names and ensure non-string types are handled
def remove_punctuation(name):
    # Ensure the value is a string and remove punctuation
    if not isinstance(name, str):
        name = str(name)  # Convert to string if not already
    return re.sub(r'[^\w\s]', '', name)

# Function to clean last names by removing suffixes and spaces
def clean_last_name(last_name):
    # List of common suffixes
    suffixes = ['jr', 'sr', 'ii', 'iii', 'iv', 'v', 'vi', 'vii', 'viii', 'ix']
    
    # Ensure last_name is a string
    last_name = str(last_name)
    
    # Remove any suffixes from the end of the last name
    parts = last_name.split()
    if parts[-1].lower() in suffixes:
        last_name = " ".join(parts[:-1])  # Remove the suffix part
    
    # Remove spaces within the last name
    last_name = last_name.replace(" ", "")
    
    return last_name

def match_position_with_fallback(player_df, player_stats, position):
    # Step 1: Handle matching for defense positions specifically (when 'def' and 'team' are the same)
    if position == 'DEF':
        merged_data = pd.merge(
            player_stats, 
            player_df[player_df['player_id'] == player_df['team']], 
            on='team',
            #left_on='team', right_on='team', 
            how='left'
        )
    else:
        # Step 2: Try to match non-defense positions by first_name, last_name, position, and team
        merged_data = pd.merge(
            player_stats, 
            player_df[player_df['position'] == position], 
            on=['first_name', 'last_name', 'team'], 
            how='left'
        )

        # Step 3: Fallback if no matches found for non-defense (search across all positions but match by team)
        if merged_data['player_id'].isnull().any():
            merged_data_all_positions = pd.merge(
                player_stats, 
                player_df, 
                on=['first_name', 'last_name', 'team'], 
                how='left', 
                suffixes=('', '_all')
            )
            # Keep the player_id from fallback, if found
            return merged_data_all_positions

    # Return the result, whether it's defense-specific or a fallback for non-defense positions
    return merged_data

# Function to process each CSV
def process_csv(csv_file, position):
    # Read the CSV for the current position
    player_stats_csv = pd.read_csv(csv_file)

    # Initialize merged_data to an empty DataFrame in case no matching logic is applied
    merged_data = pd.DataFrame()

    # Extract team abbreviations for all positions
    player_stats_csv['team'] = player_stats_csv['Player'].apply(
        lambda x: x.split(" (")[1].replace(")", "") if "(" in x else None
    )
    player_stats_csv['team'] = player_stats_csv['team'].str.upper()

    # Correct team abbreviation from 'JAC' to 'JAX'
    player_stats_csv['team'] = player_stats_csv['team'].replace('JAC', 'JAX')
    
    # Check if 'team' column was successfully created
    if 'team' not in player_stats_csv.columns or player_stats_csv['team'].isnull().all():
        raise KeyError(f"'team' column could not be created from the 'Player' field in the CSV: {csv_file}")

    if position == 'DEF':
        # Create a mapping from team acronym to player_id for defense positions
        team_mapping = players_from_db[players_from_db['position'] == 'DEF'].set_index('team')['player_id'].to_dict()
        
        # Map the team acronym to player_id
        player_stats_csv['player_id'] = player_stats_csv['team'].map(team_mapping)
        
        # Use player_stats_csv for DST without additional matching logic
        merged_data = player_stats_csv

    else:
        # For non-DST positions, proceed with the existing logic
        if position in position_mapping:
            csv_positions = position_mapping[position]
            player_stats_csv['position'] = position.upper()  # Set the position for the entire DataFrame
            player_stats_csv['position'] = player_stats_csv['position'].apply(
                lambda p: p.upper() if p.upper() in csv_positions else None
            )
        else:
            player_stats_csv['position'] = None

        # Drop rows with invalid or unmapped positions
        player_stats_csv.dropna(subset=['position'], inplace=True)

        # Split player names into first and last names
        player_stats_csv[['first_name', 'last_name']] = player_stats_csv['Player'].apply(
            lambda x: pd.Series(split_player_name(x))
        )

        # Remove punctuation from the first_name and last_name columns, and ensure lowercase
        player_stats_csv['first_name'] = player_stats_csv['first_name'].apply(lambda x: remove_punctuation(x).lower())
        player_stats_csv['last_name'] = player_stats_csv['last_name'].apply(lambda x: remove_punctuation(x).lower())

        # Clean the last names by removing suffixes and spaces
        player_stats_csv['last_name'] = player_stats_csv['last_name'].apply(clean_last_name)

        # Merge CSV with SQL Players table based on 'first_name', 'last_name', 'team', and 'position'
        merged_data = match_position_with_fallback(players_from_db, player_stats_csv, position)

        # Ensure that the 'position' column exists in unmatched_players before printing it
        if 'position' in merged_data.columns:
            unmatched_players = merged_data[merged_data['player_id'].isnull()]
            if not unmatched_players.empty:
                print(f"Unmatched players found in {position} CSV:")
                print(unmatched_players[['first_name', 'last_name', 'team', 'position']])

    # Return the merged data (with player_id from the database)
    return merged_data


# List to hold merged data from each position
all_merged_data = []

# Loop through the CSV files and process each
for position, file_path in csv_files.items():
    if os.path.exists(file_path):
        print(f"Processing {position} stats from {file_path}...")
        merged_data = process_csv(file_path, position)
        # Append the merged data to the list
        all_merged_data.append(merged_data)
        print(f"Successfully processed {position} stats!")
    else:
        print(f"File not found: {file_path}")

# Concatenate all merged data into a single DataFrame
final_merged_df = pd.concat(all_merged_data, ignore_index=True)


# Update week Here!!!!!
week = 4
# Step 2: Check if the 'week' column exists in the final_merged_df
if 'week' not in final_merged_df.columns:
    print("Week Problem!")
else:
    final_merged_df['week'].fillna(week, inplace=True)
print("Week column filled successfully!")


# Now you can insert final_merged_df into the Weekly_Stats table or perform further processing
print("All data merged successfully!")

# Close the database connection when done
db_cursor.close()
db_connection.close()


# Define the path where you want to save the CSV
output_csv_path = r"C:\Users\colet\OneDrive\Documents\FantasyDashboardProject\final_merged_stats.csv"

# Save the DataFrame to a CSV file
final_merged_df.to_csv(output_csv_path, index=False)

print(f"Final merged data saved to {output_csv_path}")



# Define the columns that match the SQL table schema
sql_columns = [
    'player_id', 'week', 'year', 'position', 'rank_', 'fg', 'fga', 'k_pct', 'lg',
    'nineteen_p', 'twenty_p', 'thirty_p', 'forty_p', 'fifty_p', 'xpt', 'xpa', 'g',
    'fpts', 'fpts_g', 'rost', 'sacks', 'd_int', 'fr', 'ff', 'def_td', 'sfty',
    'spc_td', 'p_cmp', 'p_att', 'p_pct', 'p_yds', 'p_ypa', 'p_td', 'p_int',
    'r_att', 'r_yds', 'r_td', 'fl', 'r_ypa', 're_rec', 're_tgt', 're_yds', 're_ypr',
    're_td', 're_twenty_p', 'r_twenty_p', 'team', 'first_name', 'last_name'
]

# Filter the DataFrame to include only the columns that are in the SQL table
filtered_df = final_merged_df[sql_columns]

# Filter the DataFrame to include only the columns that are in the SQL table
filtered_df = final_merged_df[sql_columns].copy()  # Use .copy() to avoid SettingWithCopyWarning

# Remove '%' from 'rost' column and convert to numeric
filtered_df.loc[:, 'rost'] = filtered_df['rost'].replace('%', '', regex=True).astype(float)
```

### Upload Weekly Stats to SQL
```python
from sqlalchemy import create_engine, text

# Define your database connection parameters
db_username = 'root'
db_password = '****'
db_host = '127.0.0.1'
db_port = '3306'
db_name = 'Fantasy_Dash2'

# Create an SQLAlchemy engine
engine = create_engine(f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")

# Upload the DataFrame to the SQL table
filtered_df.to_sql('Weekly_Stats', con=engine, if_exists='append', index=False)

print("Data uploaded successfully!")
```

### Updating Aggregate Stats Data
```python
import pymysql

# Establish connection to your MySQL database
connection = pymysql.connect(host='127.0.0.1', user='root', password='****', db='Fantasy_Dash2')

try:
    # SQL query to aggregate data from Weekly_Stats
    aggregate_query = """
    SELECT
        player_id,
        year,
        position,
        AVG(rank_) AS avg_rank_,
        SUM(fg) AS sum_fg,
        SUM(fga) AS sum_fga,
        CASE 
            WHEN SUM(fga) = 0 THEN NULL
            ELSE (SUM(fg) / SUM(fga))
        END AS avg_k_pct,
        AVG(lg) AS avg_lg,
        SUM(nineteen_p) AS sum_nineteen_p,
        SUM(twenty_p) AS sum_twenty_p,
        SUM(thirty_p) AS sum_thirty_p,
        SUM(forty_p) AS sum_forty_p,
        SUM(fifty_p) AS sum_fifty_p,
        SUM(xpt) AS sum_xpt,
        SUM(xpa) AS sum_xpa,
        SUM(g) AS sum_g,
        SUM(fpts) AS sum_fpts,
        CASE 
            WHEN SUM(g) = 0 THEN NULL
            ELSE (SUM(fpts) / SUM(g))
        END AS avg_fpts_g,
        SUM(rost) AS sum_rost,
        SUM(sacks) AS sum_sacks,
        SUM(d_int) AS sum_d_int,
        SUM(fr) AS sum_fr,
        SUM(ff) AS sum_ff,
        SUM(def_td) AS sum_def_td,
        SUM(sfty) AS sum_sfty,
        SUM(spc_td) AS sum_spc_td,
        SUM(p_cmp) AS sum_p_cmp,
        SUM(p_att) AS sum_p_att,
        CASE 
            WHEN SUM(p_att) = 0 THEN NULL
            ELSE (SUM(p_cmp) / SUM(p_att))
        END AS avg_p_pct,
        SUM(p_yds) AS sum_p_yds,
        CASE 
            WHEN SUM(p_att) = 0 THEN NULL
            ELSE (SUM(p_yds) / SUM(p_att))
        END AS avg_p_ypa,
        SUM(p_td) AS sum_p_td,
        SUM(p_int) AS sum_p_int,
        SUM(r_att) AS sum_r_att,
        SUM(r_yds) AS sum_r_yds,
        SUM(r_td) AS sum_r_td,
        SUM(fl) AS sum_fl,
        CASE 
            WHEN SUM(r_att) = 0 THEN NULL
            ELSE (SUM(r_yds) / SUM(r_att))
        END AS avg_r_ypa,
        SUM(re_rec) AS sum_re_rec,
        SUM(re_tgt) AS sum_re_tgt,
        SUM(re_yds) AS sum_re_yds,
        CASE 
            WHEN SUM(re_rec) = 0 THEN NULL
            ELSE (SUM(re_yds) / SUM(re_rec))
        END AS avg_re_ypr,
        SUM(re_td) AS sum_re_td,
        SUM(re_twenty_p) AS sum_re_twenty_p,
        SUM(r_twenty_p) AS sum_r_twenty_p,
        team,
        first_name,
        last_name
    FROM weekly_stats
    GROUP BY player_id, year, position, team, first_name, last_name;
    """

    # Execute the query to get the aggregated weekly stats
    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(aggregate_query)
        aggregated_stats = cursor.fetchall()

    # SQL queries for update or insert into Aggregate_Stats
    update_query = """
    UPDATE Aggregate_Stats
    SET 
        rank_ = (rank_ + %(avg_rank_)s) / 2,
        fg = fg + %(sum_fg)s,
        fga = fga + %(sum_fga)s,
        k_pct = CASE 
                    WHEN (fga + %(sum_fga)s) = 0 THEN NULL
                    ELSE (fg + %(sum_fg)s) / (fga + %(sum_fga)s)
                END,
        lg = (lg + %(avg_lg)s) / 2,
        nineteen_p = nineteen_p + %(sum_nineteen_p)s,
        twenty_p = twenty_p + %(sum_twenty_p)s,
        thirty_p = thirty_p + %(sum_thirty_p)s,
        forty_p = forty_p + %(sum_forty_p)s,
        fifty_p = fifty_p + %(sum_fifty_p)s,
        xpt = xpt + %(sum_xpt)s,
        xpa = xpa + %(sum_xpa)s,
        g = g + %(sum_g)s,
        fpts = fpts + %(sum_fpts)s,
        fpts_g = CASE 
                    WHEN (g + %(sum_g)s) = 0 THEN NULL
                    ELSE (fpts + %(sum_fpts)s) / (g + %(sum_g)s)
                END,
        rost = rost + %(sum_rost)s,
        sacks = sacks + %(sum_sacks)s,
        d_int = d_int + %(sum_d_int)s,
        fr = fr + %(sum_fr)s,
        ff = ff + %(sum_ff)s,
        def_td = def_td + %(sum_def_td)s,
        sfty = sfty + %(sum_sfty)s,
        spc_td = spc_td + %(sum_spc_td)s,
        p_cmp = p_cmp + %(sum_p_cmp)s,
        p_att = p_att + %(sum_p_att)s,
        p_pct = CASE 
                    WHEN (p_att + %(sum_p_att)s) = 0 THEN NULL
                    ELSE (p_cmp + %(sum_p_cmp)s) / (p_att + %(sum_p_att)s)
                END,
        p_yds = p_yds + %(sum_p_yds)s,
        p_ypa = CASE 
                    WHEN (p_att + %(sum_p_att)s) = 0 THEN NULL
                    ELSE (p_yds + %(sum_p_yds)s) / (p_att + %(sum_p_att)s)
                END,
        p_td = p_td + %(sum_p_td)s,
        p_int = p_int + %(sum_p_int)s,
        r_att = r_att + %(sum_r_att)s,
        r_yds = r_yds + %(sum_r_yds)s,
        r_td = r_td + %(sum_r_td)s,
        fl = fl + %(sum_fl)s,
        r_ypa = CASE 
                    WHEN (r_att + %(sum_r_att)s) = 0 THEN NULL
                    ELSE (r_yds + %(sum_r_yds)s) / (r_att + %(sum_r_att)s)
                END,
        re_rec = re_rec + %(sum_re_rec)s,
        re_tgt = re_tgt + %(sum_re_tgt)s,
        re_yds = re_yds + %(sum_re_yds)s,
        re_ypr = CASE 
                    WHEN (re_rec + %(sum_re_rec)s) = 0 THEN NULL
                    ELSE (re_yds + %(sum_re_yds)s) / (re_rec + %(sum_re_rec)s)
                END,
        re_td = re_td + %(sum_re_td)s,
        re_twenty_p = re_twenty_p + %(sum_re_twenty_p)s,
        r_twenty_p = r_twenty_p + %(sum_r_twenty_p)s
    WHERE player_id = %(player_id)s AND year = %(year)s AND position = %(position)s;
    """

    insert_query = """
    INSERT INTO Aggregate_Stats (
        player_id, year, position, rank_, fg, fga, k_pct, lg, nineteen_p, twenty_p, thirty_p, 
        forty_p, fifty_p, xpt, xpa, g, fpts, fpts_g, rost, sacks, d_int, fr, ff, def_td, sfty, 
        spc_td, p_cmp, p_att, p_pct, p_yds, p_ypa, p_td, p_int, r_att, r_yds, r_td, fl, r_ypa, 
        re_rec, re_tgt, re_yds, re_ypr, re_td, re_twenty_p, r_twenty_p, team, first_name, last_name
    ) 
    VALUES (
        %(player_id)s, %(year)s, %(position)s, %(avg_rank_)s, %(sum_fg)s, %(sum_fga)s, %(avg_k_pct)s, %(avg_lg)s, %(sum_nineteen_p)s, %(sum_twenty_p)s, %(sum_thirty_p)s, 
        %(sum_forty_p)s, %(sum_fifty_p)s, %(sum_xpt)s, %(sum_xpa)s, %(sum_g)s, %(sum_fpts)s, %(avg_fpts_g)s, %(sum_rost)s, %(sum_sacks)s, %(sum_d_int)s, %(sum_fr)s, %(sum_ff)s, %(sum_def_td)s, %(sum_sfty)s, 
        %(sum_spc_td)s, %(sum_p_cmp)s, %(sum_p_att)s, %(avg_p_pct)s, %(sum_p_yds)s, %(avg_p_ypa)s, %(sum_p_td)s, %(sum_p_int)s, %(sum_r_att)s, %(sum_r_yds)s, %(sum_r_td)s, %(sum_fl)s, %(avg_r_ypa)s, 
        %(sum_re_rec)s, %(sum_re_tgt)s, %(sum_re_yds)s, %(avg_re_ypr)s, %(sum_re_td)s, %(sum_re_twenty_p)s, %(sum_r_twenty_p)s, %(team)s, %(first_name)s, %(last_name)s
    );
    """

    with connection.cursor() as cursor:
        for row in aggregated_stats:
            # Try to update first, if no rows were affected then insert
            cursor.execute(update_query, row)
            if cursor.rowcount == 0:
                cursor.execute(insert_query, row)

    # Commit the transaction
    connection.commit()

finally:
    # Close the connection to the database
    connection.close()
```

### Updating Data for Combined Matchups Table
Need to update week in this code
```python
import pandas as pd
import requests
from sqlalchemy import create_engine

# Define the league ID and week number
league_id = '****'
week = 4

# Fetch rosters data
rosters_url = f"https://api.sleeper.app/v1/league/{league_id}/rosters"
response = requests.get(rosters_url)
roster_data = response.json()

if response.status_code == 200:
    print("Roster data fetched successfully.")
else:
    print(f"Error fetching roster data: {response.status_code}")
    roster_data = []

# Fetch user data
users_url = f"https://api.sleeper.app/v1/league/{league_id}/users"
user_response = requests.get(users_url)
users_data = user_response.json()

if user_response.status_code == 200:
    print("User data fetched successfully.")
else:
    print(f"Error fetching user data: {user_response.status_code}")
    users_data = []

# Map user_id to team_name
user_id_to_team_name = {
    user['user_id']: user['metadata'].get('team_name', f"Team {user['display_name']}")
    for user in users_data
}

# Process roster data
roster_entries = []
for roster in roster_data:
    roster_id = roster['roster_id']
    user_id = roster['owner_id']
    players = roster['players']
    keepers = roster.get('keepers', [])
    team_name = user_id_to_team_name.get(user_id, 'Unknown')  # Default to 'Unknown' for now

    for player_id in players:
        keeper = 1 if player_id in keepers else 0
        roster_entries.append((roster_id, player_id, keeper, team_name))

roster_df = pd.DataFrame(roster_entries, columns=['roster_id', 'player_id', 'keeper', 'team_name'])

# Fetch matchups data
matchups_url = f'https://api.sleeper.app/v1/league/{league_id}/matchups/{week}'
response = requests.get(matchups_url)

if response.status_code == 200:
    matchups_data = response.json()
    print("Matchups data fetched successfully.")
else:
    print(f"Error fetching matchups data: {response.status_code}")
    matchups_data = []

# Convert matchups data to DataFrame
df = pd.DataFrame(matchups_data)

# Process matchups data
def extract_player_roles(row):
    roles = []
    players = row['players']
    starters = row['starters']
    players_points = row['players_points']
    
    for player_id in players:
        role = 'starter' if player_id in starters else 'bench'
        points = players_points.get(str(player_id), 0)
        roles.append({
            'matchup_id': row['matchup_id'],
            'roster_id': row['roster_id'],
            'week': week,
            'year': 2024,  # Replace with actual year if available
            'player_id': player_id,
            'role': role,
            'points': points
        })
    return roles

player_roles = [extract_player_roles(row) for _, row in df.iterrows()]
player_roles_flat = [item for sublist in player_roles for item in sublist]
player_roles_df = pd.DataFrame(player_roles_flat)

# Merge player roles with roster data
matchups_with_roster_df = pd.merge(player_roles_df, roster_df, on=['roster_id', 'player_id'], how='left')

# Fill missing team_name by using the same roster_id to find existing team_name
matchups_with_roster_df['team_name'] = matchups_with_roster_df.groupby('roster_id')['team_name'].transform(lambda x: x.ffill().bfill())

# Fill missing keeper values
matchups_with_roster_df['keeper'] = matchups_with_roster_df['keeper'].fillna(0).astype(int)

# Display the first few rows of the DataFrames for inspection
print("\nMatchups DataFrame:")
print(player_roles_df.head())

print("\nCombined Matchups DataFrame with Roster Information:")
print(matchups_with_roster_df.head())

# Create a connection to the MySQL database (if you decide to upload later)
engine = create_engine('mysql+mysqlconnector://root:****@127.0.0.1/Fantasy_Dash2')
matchups_with_roster_df.to_sql('combined_matchups', con=engine, if_exists='append', index=False)
print("Data uploaded successfully.")
```

### Updating Positional Rankings
```python
from sqlalchemy import create_engine, text

# Define your database connection parameters
db_username = 'root'
db_password = '****'
db_host = '127.0.0.1'
db_port = '3306'
db_name = 'Fantasy_Dash2'

# Create an SQLAlchemy engine
engine = create_engine(f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")

# Step 1: Truncate the table
with engine.connect() as connection:
    connection.execute(text("TRUNCATE TABLE positional_rankings;"))

# Step 2: SQL command to insert data into positional_rankings using CTEs
sql_command = text(""" 
    INSERT INTO positional_rankings (week, year, position, team_name, role, total_points, position_rank, full_season_rank)
    WITH total_points AS (
        SELECT
            cm.week,
            cm.year,
            ws.position,
            cm.team_name,
            cm.role,
            ROUND(SUM(cm.points), 2) AS total_points
        FROM
            combined_matchups cm
        JOIN
            weekly_stats ws ON cm.player_id = ws.player_id AND cm.week = ws.week AND cm.year = ws.year
        GROUP BY
            cm.week, cm.year, ws.position, cm.team_name, cm.role
    ),
    ranked_positions AS (
        SELECT
            week,
            year,
            position,
            team_name,
            role,
            total_points,
            RANK() OVER (PARTITION BY week, position, role ORDER BY total_points DESC) AS position_rank
        FROM
            total_points
    ),
    season_totals AS (
        SELECT
            position,
            team_name,
            role,
            SUM(total_points) AS season_total_points
        FROM
            total_points
        GROUP BY
            position, team_name, role
    ),
    season_ranked AS (
        SELECT
            position,
            team_name,
            role,
            season_total_points,
            RANK() OVER (PARTITION BY position, role ORDER BY season_total_points DESC) AS season_rank
        FROM
            season_totals
    )
    SELECT
        rp.week,
        rp.year,
        rp.position,
        rp.team_name,
        rp.role,
        rp.total_points,
        rp.position_rank,
        sr.season_rank AS full_season_rank
    FROM
        ranked_positions rp
    JOIN
        season_ranked sr ON rp.position = sr.position AND rp.team_name = sr.team_name AND rp.role = sr.role
    WHERE
        rp.role IN ('starter', 'bench');
""")

# Step 3: Execute the SQL command to insert data
with engine.connect() as connection:
    connection.execute(sql_command)

print("Data truncated and inserted into positional_rankings successfully!")
```


### Updating Team Weekly Performance
```python
import pandas as pd
from sqlalchemy import create_engine, text

# Define your database connection parameters
db_username = 'root'
db_password = '****'
db_host = '127.0.0.1'
db_port = '3306'
db_name = 'Fantasy_Dash2'

# Create an SQLAlchemy engine
engine = create_engine(f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")

# Step 0: Drop the table if it exists to avoid conflicts
with engine.connect() as connection:
    connection.execute(text("DROP TABLE IF EXISTS team_weekly_performance;"))

# Step 1: Create the new table structure
create_table_query = """
CREATE TABLE team_weekly_performance AS
WITH team_points AS (
    SELECT 
        matchup_id,
        roster_id,
        week,
        year,
        team_name,
        SUM(points) AS points_for
    FROM combined_matchups
    WHERE role = 'starter'
    GROUP BY matchup_id, roster_id, week, year, team_name
),
matchups AS (
    SELECT 
        t1.roster_id AS team_1_roster_id,
        t1.team_name AS team_1_name,
        t1.points_for AS team_1_points_for,
        t2.roster_id AS team_2_roster_id,
        t2.team_name AS team_2_name,
        t2.points_for AS team_2_points_for,
        t1.matchup_id,
        t1.week,
        t1.year
    FROM team_points t1
    JOIN team_points t2
        ON t1.matchup_id = t2.matchup_id
        AND t1.roster_id <> t2.roster_id
        AND t1.week = t2.week
        AND t1.year = t2.year
),
team_records AS (
    SELECT DISTINCT
        t1.team_1_roster_id AS roster_id,
        t1.team_1_name AS team_name,
        t1.week,
        t1.year,
        t1.team_1_points_for AS points_for,
        t1.team_2_points_for AS points_against,
        CASE WHEN t1.team_1_points_for > t1.team_2_points_for THEN 1 ELSE 0 END AS win,
        CASE WHEN t1.team_1_points_for < t1.team_2_points_for THEN 1 ELSE 0 END AS loss,
        t1.team_1_points_for - t1.team_2_points_for AS point_diff,
        'Head-to-Head' AS record_type
    FROM matchups t1
    UNION ALL
    SELECT DISTINCT
        t2.team_2_roster_id AS roster_id,
        t2.team_2_name AS team_name,
        t2.week,
        t2.year,
        t2.team_2_points_for AS points_for,
        t2.team_1_points_for AS points_against,
        CASE WHEN t2.team_2_points_for > t2.team_1_points_for THEN 1 ELSE 0 END AS win,
        CASE WHEN t2.team_2_points_for < t2.team_1_points_for THEN 1 ELSE 0 END AS loss,
        t2.team_2_points_for - t2.team_1_points_for AS point_diff,
        'Head-to-Head' AS record_type
    FROM matchups t2
),
league_performance AS (
    SELECT DISTINCT
        tp.roster_id,
        tp.team_name,
        tp.week,
        tp.year,
        tp.points_for,
        SUM(CASE WHEN tp.points_for > opp.points_for THEN 1 ELSE 0 END) AS wins_if_played_all,
        SUM(CASE WHEN tp.points_for < opp.points_for THEN 1 ELSE 0 END) AS losses_if_played_all,
        'Vs All Teams' AS record_type
    FROM team_points tp
    CROSS JOIN team_points opp
    WHERE tp.roster_id <> opp.roster_id
      AND tp.week = opp.week
      AND tp.year = opp.year
    GROUP BY tp.roster_id, tp.team_name, tp.week, tp.year, tp.points_for
),
final_results AS (
    SELECT DISTINCT
        tr.roster_id,
        tr.team_name,
        tr.week,
        tr.year,
        ROUND(tr.points_for, 2) AS points_for,
        ROUND(tr.points_against, 2) AS points_against,
        tr.win,
        tr.loss,
        lp.wins_if_played_all,
        lp.losses_if_played_all,
        ROUND(tr.point_diff, 2) AS point_diff
    FROM team_records tr
    JOIN league_performance lp
        ON tr.roster_id = lp.roster_id
        AND tr.week = lp.week
        AND tr.year = lp.year
),
cumulative_results AS (
    SELECT
        fr.roster_id,
        fr.team_name,
        fr.week,
        fr.year,
        fr.points_for,
        fr.points_against,
        fr.win,
        fr.loss,
        fr.wins_if_played_all,
        fr.losses_if_played_all,
        fr.point_diff,
        SUM(fr.win) OVER (PARTITION BY fr.roster_id ORDER BY fr.year, fr.week ROWS UNBOUNDED PRECEDING) AS cum_win_h2h,
        SUM(fr.loss) OVER (PARTITION BY fr.roster_id ORDER BY fr.year, fr.week ROWS UNBOUNDED PRECEDING) AS cum_loss_h2h,
        SUM(fr.wins_if_played_all) OVER (PARTITION BY fr.roster_id ORDER BY fr.year, fr.week ROWS UNBOUNDED PRECEDING) AS cum_win_all,
        SUM(fr.losses_if_played_all) OVER (PARTITION BY fr.roster_id ORDER BY fr.year, fr.week ROWS UNBOUNDED PRECEDING) AS cum_loss_all,
        ROUND(SUM(fr.point_diff) OVER (PARTITION BY fr.roster_id ORDER BY fr.year, fr.week ROWS UNBOUNDED PRECEDING), 2) AS cum_point_diff
    FROM final_results fr
),
main_records AS (
    SELECT
        roster_id,
        team_name,
        SUM(win) AS main_win_h2h,
        SUM(loss) AS main_loss_h2h,
        SUM(wins_if_played_all) AS main_win_all,
        SUM(losses_if_played_all) AS main_loss_all
    FROM final_results
    GROUP BY roster_id, team_name
)
SELECT
    cr.roster_id,
    cr.team_name,
    cr.week,
    cr.year,
    cr.points_for,
    cr.points_against,
    cr.win,
    cr.loss,
    cr.wins_if_played_all,
    cr.losses_if_played_all,
    cr.point_diff,
    CONCAT(cr.cum_win_h2h, '-', cr.cum_loss_h2h) AS head_to_head_record,
    CONCAT(cr.cum_win_all, '-', cr.cum_loss_all) AS all_time_record,
    ROUND(cr.cum_point_diff, 2) AS cumulative_point_diff,
    CONCAT(mr.main_win_h2h, '-', mr.main_loss_h2h) AS main_head_to_head,
    CONCAT(mr.main_win_all, '-', mr.main_loss_all) AS main_cumulative
FROM cumulative_results cr
JOIN main_records mr
    ON cr.roster_id = mr.roster_id
ORDER BY cr.year, cr.week, cr.team_name;
"""

# Step 2: Execute the create table query
with engine.connect() as connection:
    connection.execute(text(create_table_query))

# Step 3: Confirm that the table was created successfully
with engine.connect() as connection:
    result = connection.execute(text("SHOW TABLES;"))
    tables = [row[0] for row in result]
    print("Tables in the database:", tables)
```
