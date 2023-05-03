#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 11 09:45:43 2023

@author: nicolasgutierrez
"""

# SQL for FDA 

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
        File path of FDA dataset.

    Returns
    -------
    df : pd.dataframe
        This is the FDA dataframe.

    """
    df = pd.read_csv(filename)
    df = df.apply(lambda x: x.astype(str).str.lower())
    
    aaplicant = pd.DataFrame(df['applicant'])
    
    Label_encoder = preprocessing.LabelEncoder() 
    df['applicant_id'] = df.apply(Label_encoder.fit_transform)['applicant']+1 #do it the sql way 
    aaplicant['id'] = df['applicant_id']
    everything = pd.DataFrame(df[['proper_name','proprietary_name','license_no','applicant_id']]) 
    return aaplicant, everything 
    

def append_to_db(df1: pd.DataFrame, df2: pd.DataFrame):
    # do some operations to split affiliations and authors
    #Create an engine and let pandas help with sql 
    engine = sqlalchemy.create_engine(
        'sqlite:////Users/nicolasgutierrez/Desktop/Databases/FDA.db') 
    with engine.connect() as conn:
        df1.to_sql('applicant', conn, index=False, if_exists='append')
        df2.to_sql('everything', conn, index=False, if_exists='append')
        #df1.to_sql('loan_intent', engine, index=False, if_exists='append')
        #df2.to_sql('everything', engine, index=False, if_exists='append')  

if __name__ =='__main__': 
    sqliteConnection = sqlite3.connect('FDA.db') 
    cursor = sqliteConnection.cursor() 

    applicant = '''
    CREATE TABLE IF NOT EXISTS applicant ( 
        id INTEGER PRIMARY KEY,
        applicant VARCHAR(250) NOT NULL 
    );''' 

    everything = '''
    CREATE TABLE IF NOT EXISTS everything (
        bla_nda_number INTEGER PRIMARY KEY,  
        proper_name VARCHAR(250),
        proprietary_name VARCHAR(250),
        license_no INTEGER(250),
        applicant_id INTEGER,
        FOREIGN KEY(applicant_id) REFERENCES applicant(id) 
        );''' 

    cursor.execute(applicant)
    cursor.execute(everything)
    cursor.close()
    
    applicantdf, everythingdf = read('FDA.csv') 
    applicantdf = applicantdf.drop_duplicates()
    append_to_db(applicantdf, everythingdf)
#    credit_risk = read('/Users/nicolasgutierrez/Desktop/Databases/data/credit_risk_dataset.csv') 
