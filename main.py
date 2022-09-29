#!/usr/bin/env python
# -*- coding: utf-8 -*-
from DbConnector import DbConnector
from tabulate import tabulate

import os
import csv
import time

class Task1:

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

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows
    
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
            print(" || User inserted in {:.2f} seconds".format(stop-start))
            total_time += stop - start
        print('Data inserted in {:0.0f} minutes and seconds {:0.0f}'.format(total_time/60, total_time%60))


def main():
    try:
        program = Task1()                           # Initialize database connection
        program.delete_tables()
        program.create_tables()                     # Create database tables if they don't exist
        program.insert_data('dataset')              # Parse dataset and insert data into tables
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()   # Close database connection after program has finished or failed



if __name__ == "__main__":
    main()