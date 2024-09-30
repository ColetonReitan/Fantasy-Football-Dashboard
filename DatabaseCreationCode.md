```python
import mysql.connector

# Establish a connection to the MySQL server
connection = mysql.connector.connect(
    host='127.0.0.1',
    user='root',
    password='****',
    database='Fantasy_Dash2'
)

cursor = connection.cursor()

# Define the SQL statements to create tables
create_players_table_sql = """
CREATE TABLE IF NOT EXISTS Players (
    player_id VARCHAR(50) PRIMARY KEY,
    last_name VARCHAR(255),
    first_name VARCHAR(255),
    team VARCHAR(255),
    position VARCHAR(50),
    height INT,
    weight INT,
    birth_date DATE,
    college VARCHAR(255),
    status VARCHAR(50),
    age INT,
    number INT,
    years_exp INT
);
"""

create_users_table_sql = """
CREATE TABLE IF NOT EXISTS Users (
    user_id BIGINT PRIMARY KEY,
    display_name VARCHAR(50) NOT NULL UNIQUE,
    avatar VARCHAR(100) NOT NULL UNIQUE,
    league_id bigint
);
"""

create_league_table_sql = """
CREATE TABLE IF NOT EXISTS League (
    league_id BIGINT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    season INT NOT NULL,
    total_rosters int NOT NULL,
);
"""

create_weekly_stats_table_sql = """
CREATE TABLE IF NOT EXISTS Weekly_Stats (
stat_id int AI PK 
player_id varchar(50) 
week int 
year year 
position varchar(10) 
rank_ decimal(10,2) 
fg decimal(10,2) 
fga decimal(10,2) 
k_pct decimal(10,2) 
lg decimal(10,2) 
nineteen_p decimal(10,2) 
twenty_p decimal(10,2) 
thirty_p decimal(10,2) 
forty_p decimal(10,2) 
fifty_p decimal(10,2) 
xpt decimal(10,2) 
xpa decimal(10,2) 
g decimal(10,2) 
fpts decimal(10,2) 
fpts_g decimal(10,2) 
rost decimal(10,2) 
sacks decimal(10,2) 
d_int decimal(10,2) 
fr decimal(10,2) 
ff decimal(10,2) 
def_td decimal(10,2) 
sfty decimal(10,2) 
spc_td decimal(10,2) 
p_cmp decimal(10,2) 
p_att decimal(10,2) 
p_pct decimal(10,2) 
p_yds decimal(10,2) 
p_ypa decimal(10,2) 
p_td decimal(10,2) 
p_int decimal(10,2) 
r_att decimal(10,2) 
r_yds decimal(10,2) 
r_td decimal(10,2) 
fl decimal(10,2) 
r_ypa decimal(10,2) 
re_rec decimal(10,2) 
re_tgt decimal(10,2) 
re_yds decimal(10,2) 
re_ypr decimal(10,2) 
re_td decimal(10,2) 
re_twenty_p decimal(10,2) 
r_twenty_p decimal(10,2) 
team varchar(255) 
first_name varchar(255) 
last_name varchar(255)
);
"""

create_aggregate_stats_table_sql = """
CREATE TABLE IF NOT EXISTS Aggregate_Stats (
agg_stat_id int AI PK 
player_id varchar(50) 
year year 
position varchar(225) 
rank_ decimal(10,2) 
fg decimal(10,2) 
fga decimal(10,2) 
k_pct decimal(10,2) 
lg decimal(10,2) 
nineteen_p decimal(10,2) 
twenty_p decimal(10,2) 
thirty_p decimal(10,2) 
forty_p decimal(10,2) 
fifty_p decimal(10,2) 
xpt decimal(10,2) 
xpa decimal(10,2) 
g decimal(10,2) 
fpts decimal(10,2) 
fpts_g decimal(10,2) 
rost decimal(10,2) 
sacks decimal(10,2) 
d_int decimal(10,2) 
fr decimal(10,2) 
ff decimal(10,2) 
def_td decimal(10,2) 
sfty decimal(10,2) 
spc_td decimal(10,2) 
p_cmp decimal(10,2) 
p_att decimal(10,2) 
p_pct decimal(10,2) 
p_yds decimal(10,2) 
p_ypa decimal(10,2) 
p_td decimal(10,2) 
p_int decimal(10,2) 
r_att decimal(10,2) 
r_yds decimal(10,2) 
r_td decimal(10,2) 
fl decimal(10,2) 
r_ypa decimal(10,2) 
re_rec decimal(10,2) 
re_tgt decimal(10,2) 
re_yds decimal(10,2) 
re_ypr decimal(10,2) 
re_td decimal(10,2) 
re_twenty_p decimal(10,2) 
r_twenty_p decimal(10,2) 
team varchar(255) 
first_name varchar(255) 
last_name varchar(255)
);
"""

# Function to execute SQL statements
def create_table(sql, table_name):
    try:
        cursor.execute(sql)
        print(f"Table '{table_name}' created successfully.")
    except mysql.connector.Error as err:
        print(f"Error creating table '{table_name}': {err}")

# Create tables
create_table(create_players_table_sql, 'Players')
create_table(create_users_table_sql, 'Users')
create_table(create_teams_table_sql, 'Teams')
create_table(create_leagues_table_sql, 'Leagues')
create_table(create_rosters_table_sql, 'Rosters')
create_table(create_matchups_table_sql, 'Matchups')
create_table(create_transactions_table_sql, 'Transactions')
create_table(create_drafts_table_sql, 'Drafts')
create_table(create_draft_picks_table_sql, 'Draft_Picks')
create_table(create_weekly_stats_table_sql, 'Weekly_Stats')
create_table(create_aggregate_stats_table_sql, 'Aggregate_Stats')

# Close the cursor and connection
cursor.close()
connection.close()
```
