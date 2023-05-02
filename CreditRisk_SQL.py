#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr  2 18:58:51 2023

@author: nicolasgutierrez
"""

#Import packages 
import sqlite3 
import sqlalchemy
import pandas as pd
from sklearn import preprocessing 


def read(filename:str):
    """
    Parameters
    ----------
    filename : str
        File path of credit risk dataset.

    Returns
    -------
    df : pd.dataframe
        This is the credit risk dataframe.

    """
    df = pd.read_csv(filename)
    df = df.apply(lambda x: x.astype(str).str.lower())
    
    loan_intentions = pd.DataFrame(df['loan_intent'])
    
    Label_encoder = preprocessing.LabelEncoder()
    df['loan_intent'] = df.apply(Label_encoder.fit_transform)['loan_intent']+1 #do it the sql way 
    everything = pd.DataFrame(df[['person_age','person_income','loan_intent']]) 
    return loan_intentions, everything
    

def append_to_db(df1: pd.DataFrame, df2: pd.DataFrame):
    # do some operations to split affiliations and authors
    #Create an engine and let pandas help with sql 
    engine = sqlalchemy.create_engine(
        'sqlite:////Users/nicolasgutierrez/Desktop/Databases/credit_risk.db') 
    with engine.connect() as conn:
        df1.to_sql('loan_intent', conn, index=False, if_exists='append')
        df2.to_sql('everything', conn, index=False, if_exists='append')
        #df1.to_sql('loan_intent', engine, index=False, if_exists='append')
        #df2.to_sql('everything', engine, index=False, if_exists='append')

if __name__ =='__main__': 
    sqliteConnection = sqlite3.connect('credit_risk.db') 
    cursor = sqliteConnection.cursor() 

    loan_intent = '''
    CREATE TABLE IF NOT EXISTS loan_intent (
        id INTEGER PRIMARY KEY,
        loan_intent VARCHAR(250) NOT NULL
    );''' 

    everything = '''
    CREATE TABLE IF NOT EXISTS everything (
        id INTEGER PRIMARY KEY, 
        person_age INTEGER(250),
        person_income INTEGER(250),
        loan_intent VARCHAR(250),
        loan_intent_id INTEGER, 
        FOREIGN KEY(loan_intent_id) REFERENCES loan_intent(id) 
        );''' 

    cursor.execute(loan_intent)
    cursor.execute(everything)
    cursor.close()
    
    loan_intentdf, everythingdf = read('credit_risk_dataset.csv')
    loan_intentdf = loan_intentdf.drop_duplicates()
    append_to_db(loan_intentdf, everythingdf)
#    credit_risk = read('/Users/nicolasgutierrez/Desktop/Databases/data/credit_risk_dataset.csv') 









