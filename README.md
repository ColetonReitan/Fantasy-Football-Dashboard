# Fantasy Football Dashboard

## Project Overview
This is a personal project I took on for the benefit of myself as well as the rest of my fantasy football league members. The goal is to create an up-to-date dashboard that helps owners better understand and manage their fantasy teams.   

In this project, a database is created in MySQL Workbench 8.0 using data pulled from Sleeper and ESPN API's. The database is connected to Tableau that has a custom dashboard built to better understand and display the data.   

The dashboard link can be found here: https://public.tableau.com/app/profile/coleton.reitan7808/viz/OuahFantasyLeague/TeamDash

### Dashbaord Screenshot

![](Dash_Images/dash_ss.png)

## Creation Process
This was a multi-step process that involved creating a database from scratch in MySQL, importing and cleaning data from Sleeper and ESPN API's, uploading data into the MySQL database, and finally creating the dashboard.  
Following the dashboard creation, it is updated every week for new fantasy league data. 

### 1) Creating SQL Database
SQL commands were ran in Jupyter Notebook with Python using the mysql.connector() package.

The following tables were created in the database
[To see complete code click here](DatabaseCreationCode.md)

- Aggregate Stats    
      Holds player total season stats (in game stats such as rushing yards, passing yards, etc.)
- Weekly Stats    
      Holds player stats by week (in game stats such as rushing yards, passing yards, etc.)
- Players    
      Holds player non-game information (such as sleeper's player id, height, weight, etc.)
- League    
      Holds league information (such as league id, name, etc.)
- Users    
      Holds user information (such as user id, display name, etc.)
- Team Weekly Performance    
      Holds fantasy team performance by week (such as roster id, points for, record, cumulative record, etc.)
- Combined Mathcups    
      Holds fantasy matchup information (such as matchup id, week, player id, etc.)
- Positional Rankings    
      Holds player performance ranking information by week (such as position, team name, total points, etc.)
