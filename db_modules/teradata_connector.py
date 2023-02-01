
# coding: utf-8

# In[44]:

import pyodbc
import getpass
import sys_queries as sys
import pandas as pd
import numpy as np
import teradata as td
import security_notebook as security
import configparser 
import json

# In[59]:

CONFIG = configparser.ConfigParser()
CONFIG.read("config.ini")

METHOD, AUTH,SERVER,DRIVER = CONFIG.get("TERADATA","method"),CONFIG.get("TERADATA","authentication"),CONFIG.get("TERADATA","server"),CONFIG.get("TERADATA","driver")
    
USER,KEY = CONFIG.get("CREDENTIALS","user"), CONFIG.get("CREDENTIALS","key_phrase")    

score_object = []


COLUMNS = ["COLUMNS"]   

def connect_Teradata():
    udaExec = td.UdaExec(appName="sys", version="1.0", logConsole=False, logLevel='ERROR')
    connection = udaExec.connect(method=METHOD, authentication=AUTH,system=SERVER, driver=DRIVER,username=USER,password=security.get_cred(KEY,USER));
    
    return connection

def sys_dataframe():
    try:
        df = pd.read_sql(sys.get_cases_past_6_months,connect_Teradata())
        set_data_types(df)
        strip_df(df)
        return df
    except Exception as e:
        print(e)

def strip_df(df):
    columns = ['COLUMNS']
    for c in columns:
        df[c] = df[c].str.lstrip()
        df[c] = df[c].str.rstrip()
        
        
def set_data_types(df):
    dtypes ={'COLUMN' : 'TYPE'}
    for column in dtypes:
        df[column] = df[column].astype(dtypes[column])        
        
def get_case_id_param(COLUMNS_PARAMETERS):
    res = session.execute(COLUMNS_PARAMETERS).fetchone()
    if res:
        return res
    else:
        return np.nan

def get_score(df):
    score = 0
    '<<BSUINESS_LOGIC>>'
    return int(score)   

def create_match_object(index,value):
    score_object.append(index + ":" + value)

def get_match_object():
    match_object = "|".join(str(x) for x in score_object)
    score_object.clear()
    return match_object
    
def date_match(alert_date,payload_date):
    return is_match(int((pd.to_datetime(alert_date) - pd.to_datetime(payload_date)).days))

def COLUMN_match(bene_sort_code,bmp_target_sort_code):
    return True if bene_sort_code == bmp_target_sort_code else False 

def COLUMN_2_match(COLUMN_1,COLUMN_2):
    return True if COLUMN_1 == COLUMN_2 else False

def is_match(days):
    if days <= 0 and days >= -2:
        return True
    else: 
        return False 

    
