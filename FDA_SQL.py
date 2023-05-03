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
        
def create_bridge (df3: pd.DataFrame):
    '''
    Creating a bridge dataframe that will assist in the deduplication process
    
    Args:
        df(pd.DataFrame): applicants df, that reference the applicant table
    Returns:
        pandas bridge DataFrame 
    '''
    bridgedf = pd.Dataframe(columns=['parent_id','applicant_id'])
    bridgedf['parent_id'] = df3['id']
    bridgedf['applicant_id'] = df3['id']
    
    for i, app1 in df3.iterrows():
        for j, app2 in df3.iloc[i+1:].iterrows():
            if app1['applicant'] in app2['applicant']:
                bridgedf['parent_id'][j] = df3['id'][i]
            elif app2['applicant'] in app1['applicant']:
                bridgedf['parent_id'][i] = df3['id'][j]
    return bridgedf
                

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
    
    bridge = '''
    CREATE TABLE IF NOT EXISTS bridge (
        applicant_id INTEGER,
        parent_id INTEGER,
        FOREIGN KEY(parent_id) REFERENCES applicant(id)
        );'''
    
    cursor.execute(applicant)
    cursor.execute(everything)
    cursor.execute(bridge)
    cursor.close()
    
    applicantdf, everythingdf = read('FDA.csv') 
    applicantdf = applicantdf.drop_duplicates()
    append_to_db(applicantdf, everythingdf)
#    credit_risk = read('/Users/nicolasgutierrez/Desktop/Databases/data/credit_risk_dataset.csv') 
