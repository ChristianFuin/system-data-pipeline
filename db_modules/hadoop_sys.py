#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys2_queries as sys2
import pandas as pd
import numpy as np
import teradata as td
import security_notebook as security
import configparser
from impala.dbapi import connect


# In[2]:


CONFIG = configparser.ConfigParser()
CONFIG.read("config.ini")

AUTH,SERVER,PORT = CONFIG.get("HADOOP","authentication"),CONFIG.get("HADOOP","server"),CONFIG.get("HADOOP","port")
USER,KEY = CONFIG.get("CREDENTIALS","user"), CONFIG.get("CREDENTIALS","key_phrase")    


# In[56]:


def get_hadoop_cursor():
     try:
        cursor = connect(host=SERVER,port=PORT,auth_mechanism=AUTH,user=USER,password=security.get_cred(KEY,USER)).cursor()
        return cursor
     except Exception as e:
        print(e)
        
def close_cursor(cursor):
    try:
        cursor.close()
    except Exception as e:
        print(e)
    
def get_customer_cin(COLUM,COLUMN):
    try:
        cursor.execute(sys2.get_cust_cin,COLUMN)
        return cursor.fetchone()[0]
    except Exception as e:
        print(e)
