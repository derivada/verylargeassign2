#!/usr/bin/env python
# -*- coding: utf-8 -*-
from DbConnector import DbConnector
from tabulate import tabulate

class Task1:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self):
        # check if tables where already created
        query_users = """CREATE TABLE IF NOT EXISTS User (
            id VARCHAR(30) NOT NULL PRIMARY KEY,
            has_labels BOOLEAN NOT NULL)
        """
        query_activity = """CREATE TABLE IF NOT EXISTS Activity (
            id INT NOT NULL PRIMARY KEY,
            user_id VARCHAR(30) NOT NULL,
            transportation_mode VARCHAR(30) NOT NULL,
            start_date_time DATETIME  NOT NULL,
            end_date_time DATETIME,
            FOREIGN KEY (user_id)
                REFERENCES User(id))
        """
        query_trackpoint = """CREATE TABLE IF NOT EXISTS TrackPoint (
            id INT NOT NULL PRIMARY KEY,
            activity_id INT NOT NULL,
            lat DOUBLE NOT NULL,
            lon DOUBLE NOT NULL,
            altitude INT NOT NULL,
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
        query_delete_trackpoint = """DROP TABLE IF EXISTS TrackPoint
        """
        query_delete_activity = """DROP TABLE IF EXISTS Activity
        """
        query_delete_user = """ DROP TABLE IF EXISTS User
        """
        self.cursor.execute(query_delete_trackpoint)
        self.cursor.execute(query_delete_activity)
        self.cursor.execute(query_delete_user)
        self.db_connection.commit()

def main():
    program = None
    try:
        program = Task1()
        program.create_tables()
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == "__main__":
    main()