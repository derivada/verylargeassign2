#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pickle import TRUE
from DbConnector import DbConnector
from tabulate import tabulate

import os
import csv
import time

class Connection:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self):
        # We create Users, Activity and TrackPoint tables following the database schema provided 
        query_users = """CREATE TABLE IF NOT EXISTS User (
            id VARCHAR(30) NOT NULL PRIMARY KEY,
            has_labels BOOLEAN NOT NULL)
        """
        query_activity = """CREATE TABLE IF NOT EXISTS Activity (
            id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            user_id VARCHAR(30) NOT NULL,
            transportation_mode VARCHAR(30),
            start_date_time DATETIME NOT NULL,
            end_date_time DATETIME NOT NULL,
            FOREIGN KEY (user_id)
                REFERENCES User(id))
        """
        query_trackpoint = """CREATE TABLE IF NOT EXISTS TrackPoint (
            id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            activity_id INT NOT NULL,
            lat DOUBLE NOT NULL,
            lon DOUBLE NOT NULL,
            altitude INT,
            date_time DATETIME NOT NULL,
            FOREIGN KEY (activity_id)
                REFERENCES Activity(id))
        """
        self.cursor.execute(query_users)
        self.cursor.execute(query_activity)
        self.cursor.execute(query_trackpoint)
        self.db_connection.commit()
        print('Tables User, Activity and TrackPoint created!')
    
    def delete_tables(self):
        # We delete all the tables in the correct order
        query_delete_trackpoint = """DROP TABLE IF EXISTS TrackPoint"""
        query_delete_activity = """DROP TABLE IF EXISTS Activity"""
        query_delete_user = """DROP TABLE IF EXISTS User"""
        self.cursor.execute(query_delete_trackpoint)
        self.cursor.execute(query_delete_activity)
        self.cursor.execute(query_delete_user)
        self.db_connection.commit()
    
    def insert_data(self, dataset_path):
        # Find labeled users
        labeled_users = []
        with open(dataset_path + '\\labeled_ids.txt', newline = '') as csvfile:
            reader = csv.reader(csvfile, delimiter = '\t', quotechar = '|')
            for row in reader:
                labeled_users.append(row[0])
        users = [f.path.split('\\')[2] for f in os.scandir(dataset_path + '\\data') if f.is_dir()]
        activity_counter = 1
        total_time = 0
        for user in users:
            # Insert user
            start = time.perf_counter()
            print('Current user = ', user, end = '')
            query_insert_user = """INSERT INTO User (id, has_labels) VALUES ({}, {})"""
            if labeled_users.count(user) > 0:
                self.cursor.execute(query_insert_user.format(user, 'TRUE'))
            else:
                self.cursor.execute(query_insert_user.format(user, 'FALSE'))
            # Read labels file
            labels = []
            if labeled_users.count(user) > 0:
                with open(dataset_path + '\\data\\{}\\labels.txt'.format(user), newline='') as csvfile:
                    reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
                    line_count = 0
                    for row in reader:
                        line_count += 1
                        if line_count == 1: continue
                        labels.append(row)
            
            # Activity files
            for file in os.scandir(dataset_path + '\\data\\{}\\Trajectory'.format(user)):
                trackpoints = []
                with open(file.path, newline='') as csvfile:
                    reader = csv.reader(csvfile, delimiter=',', quotechar='|')
                    line_count = 0
                    # Read lines
                    for row in reader:
                        line_count += 1
                        if(line_count <= 6): continue # Header
                        elif(line_count > 2506): break # File too large
                        trackpoints.append(row)
                    
                    if(line_count > 2506): continue # File too large -> Next activity
                    
                    # Insert activity and trackpoints into DB
                    # Formato DATETIME: YY5YY-MM-DD hh:mm:ss
                    # Get activity attributes (including transportation mode)
                    query_insert_activity = """INSERT INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time) VALUES ({}, {}, {}, {}, {})"""
                    start_date_time = (trackpoints[0][5] + ' ' + trackpoints[0][6]).replace('/', '-')
                    end_date_time = (trackpoints[len(trackpoints)-1][5] + ' ' + trackpoints[len(trackpoints)-1][6]).replace('/', '-')
                    transportation_mode = 'NULL'
                    for label in labels:
                        if start_date_time == label[0].replace('/', '-') and end_date_time == label[1].replace('/', '-'):
                            transportation_mode = '\'' + label[2] + '\'' 
                    # Insert activity
                    self.cursor.execute(query_insert_activity.format(0, user, transportation_mode, '\'' + start_date_time + '\'' , '\'' + end_date_time + '\''))
                    query_insert_trackpoint = """INSERT INTO TrackPoint (id, activity_id, lat, lon, altitude, date_time) VALUES """
                    trackpoint_values = """({}, {}, {}, {}, {}, {}), """
                    for trackpoint in trackpoints:
                        date_time = ('\'' + trackpoint[5] + ' ' + trackpoint[6] + '\'').replace('/', '-')
                        query_insert_trackpoint = query_insert_trackpoint + trackpoint_values.format(0, activity_counter, trackpoint[0], trackpoint[1], trackpoint[3], date_time)
                    query_insert_trackpoint = query_insert_trackpoint[:-2] + ';'
                    self.cursor.execute(query_insert_trackpoint)
                    activity_counter += 1
            self.db_connection.commit()
            stop = time.perf_counter()
            print(" || User inserted in {:.2f} seconds".format(stop - start))
            total_time += stop - start
        print('Data inserted in {:0.0f} minutes and seconds {:0.0f}'.format(total_time / 60, total_time % 60))
    
    def execute_and_print(self, query, message):
        # Helper function that executes and prints the results of a query
        start = time.perf_counter()
        self.cursor.execute(query)
        stop = time.perf_counter()
        rows = self.cursor.fetchall()
        print(message + '\n')
        print(tabulate(rows, headers=self.cursor.column_names, tablefmt = "pretty"))
        if len(rows) == 1:
            print('\n1 row in set ({:.2f} sec)\n'.format(stop - start))
        else:
            print('\n{} rows in set ({:.2f} sec)\n'.format(len(rows), stop - start))
    
    def query_2(self):
        # 2: Find the average number of activities per user.
        # We perform a subquery to get a table with the number of activities grouped by user, 
        # then we aggregate the count column and perform an average
        query = "SELECT AVG(count) as Average FROM (SELECT user_id, COUNT(id) as count FROM Activity GROUP BY user_id) AS count_act;"
        self.execute_and_print(query, "Query 2 - Find the average number of activities per user:")
    
    def query_4(self):
        # 4: Find all users who have taken a taxi.
        # We find all activites where a taxi was used and then count the distinct user IDs found on those activies
        query = "SELECT COUNT(DISTINCT(user_id)) as 'Taxi Users' FROM Activity WHERE transportation_mode = 'taxi';"
        self.execute_and_print(query, "Query 4 - Find all users who have taken a taxi:")
    def query_6(self):
        # 6:
        #   a) Find the year with the most activities.
        #   b) Is this also the year with most recorded hours?

        # For this task, we will make the simplification that a task initiated in a year also ends in that year. This is most notable on task b),
        # where the task hours only count towards the year in which it starts. Without this, the queries would become way harder to write and understand.

        # a) For the first query, we group all activies by their start year, and then aggregate by their count. To get the maximum one,
        # we use the trick of ordering by the count and then limiting the query to 1 row. Of course, this trick is only acceptable since
        # we have a small number of output rows, and it would be inefficient for big intermediate tables (sorting is O(nlogn), max is O(n)) 
        query = "SELECT Year(start_date_time) as Year, COUNT(id) as 'Activity Count' FROM Activity GROUP BY Year ORDER BY 'Activity Count' DESC LIMIT 1;"
        self.execute_and_print(query, "Query 6 - a) Find the year with the most activities:")
        # b) For this part, we modify the above query by now counting the sum of the difference of hours between
        query = "SELECT Year(start_date_time) as Year, SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) as 'Total Hours' from Activity GROUP BY Year ORDER BY 'Total Hours' DESC LIMIT 1;"
        self.execute_and_print(query, "Query 6 - b) Is this also the year with most recorded hours?:")
        print('As we see, 2008 with the most activities, but 2009 has more hours recorded\n')
    
    def query_8(self):
        # 8: Find the top 20 users who have gained the most altitude meters.
        #   - Output should be a table with (id, total meters gained per user)
        #   - Remember that some altitude-values are invalid
        #   - Tip: SUM (tp_n.altitude - tp_n-1.altitude), tp_n.altitude > tp_n-1.altitude

        # For this query, we first get all user ids and altitude from a join of TrackPoint and Activity, filtering invalid altitudes (marked as -777)
        query = "SELECT tp.id, act.user_id, tp.altitude FROM TrackPoint as tp INNER JOIN Activity as act on tp.id = act.id WHERE tp.altitude != -777"
        start = time.perf_counter()
        self.cursor.execute(query)
        last = None
        user_altitudes = {}
        # Now, we compute the substraction of consecutive pairs on all rows
        for (id, user_id, altitude) in self.cursor:
            if(last == None or last[1] != user_id or last[2] > altitude):
                # First trackpoint OR change of user OR altitude difference negative
                last = (id, user_id, altitude)
                continue
            if(user_id not in user_altitudes):
                user_altitudes[user_id] = altitude - last[2]
            else:
                user_altitudes[user_id] += altitude - last[2]
            last = (id, user_id, altitude)
        # And we sort by most feet gained
        user_altitudes =  dict(sorted(user_altitudes.items(), key=lambda item: item[1], reverse=1))
        stop = time.perf_counter()
        # The rest is just arranging the result as a table
        top = 1
        rows = []
        for entry in user_altitudes:
            row = []
            row.append(top)
            row.append(entry)
            row.append(user_altitudes[entry])
            rows.append(row)
            top += 1
            if(top > 20): break
        print("Query 8 - Find the top 20 users who have gained the most altitude meters:\n")
        print(tabulate(rows, headers=["Top", "User ID", "Altitude gained"], tablefmt = "pretty"))
        if len(rows) == 1:
            print('\n1 row in set ({:.2f} sec)\n'.format(stop - start))
        else:
            print('\n{} rows in set ({:.2f} sec)\n'.format(len(rows), stop - start))

    def query_10(self):
        # 10: Find the users who have tracked an activity in the Forbidden City of Beijing.
        #   - In this question you can consider the Forbidden City to have
        #   coordinates that correspond to: lat 39.916, lon 116.397.

        # For this query, we join TrackPoint and Activity on the activity ID and filter by the coordinates (with a tolerance of 0.001)
        # For displaying the result, we print unique user_ids (the same can be accomplished by using group by user_id)
        query = """
           SELECT DISTINCT(user_id) as 'Users in Forbidden City' 
           FROM TrackPoint as tp JOIN Activity as act 
           WHERE tp.activity_id = act.id AND tp.lat BETWEEN 39.915 AND 39.917 AND tp.lon BETWEEN 116.396 AND 116.398"""
        self.execute_and_print(query, "Query 10 - Find users who have tracked an activity in the Forbidden City of Beijing:")




        

def main():
    try:
        program = Connection()                           # Initialize database connection
        #program.delete_tables()
        #program.create_tables()                     # Create database tables if they don't exist
        #program.insert_data('dataset')              # Parse dataset and insert data into tables
        # Execute the queries
        program.query_2()
        program.query_4()
        program.query_6()
        program.query_8()
        program.query_10()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()   # Close database connection after program has finished or failed



if __name__ == "__main__":
    main()