#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 09:25:17 2023

@author: nicolasgutierrez
"""

#Step1; Create a place for the data and identify relational model 
# Order of operation matters 


import sqlite3 
import pandas as pd
import sqlalchemy 
#def make_db():


aff_query = '''
CREATE TABLE IF NOT EXISTS affiliations (
    id INTEGER PRIMARY KEY,
    name VARCHAR(250) NOT NULL,
    zip INT
);''' 

author_query = '''
CREATE TABLE IF NOT EXISTS authors (
    id INTEGER PRIMARY KEY,
    first_name VARCHAR(250),
    last_name VARCHAR(250) NOT NULL,
    affiliations_id INTEGER,
    FOREIGN KEY(affiliations_id) REFERENCES affiliations(id)
    );''' 

#Create connection and cursor (unit that does operations in sql) 
sqliteConnection = sqlite3.connect('test_sqlite.db') 
cursor = sqliteConnection.cursor()
print("Database created and Succesfully Connected to SQLite")

sqlite_select_Query = "select sqlite_version();"
cursor.execute(sqlite_select_Query)
record = cursor.fetchall()
print("SQLite Database Version is: ", record)

cursor.execute(aff_query)
cursor.execute(author_query)

cursor.close() 

def append_to_db(df: pd.DataFrame):
    # do some operations to split affiliations and authors
    #Create an engine and let pandas help with sql 
    engine = sqlalchemy.create_engine(
        'sqlite://Users/nicolasgutierrez/Desktop/Databases/live_test_sqlite.db') 
    df.to_sql('aff', engine, index=False, if_exists='append')
    
#Have to account for SQLAlchemy v2
