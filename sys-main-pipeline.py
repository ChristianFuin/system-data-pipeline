import pandas as pd
from collections import Counter
from pathlib import Path
import numpy as np
from impala.util import as_pandas
from impala.dbapi import connect
import configparser
import security_notebook as security
import teradata_connector as tc
import datedata3 as datedata3
import logging
import re
import aws_connector as aws
import sys_queries
import messenger as sms
import error_logger as logger

#Settings
dataframe_path = Path("SET_YOUR_PATH")
load_data = dataframe_path / 'FILE.csv'
TODAY = datedata3.date.today()
CONFIG = configparser.ConfigParser()
CONFIG.read("config.ini")
AUTH,SERVER,PORT = CONFIG.get("HADOOP","authentication"),CONFIG.get("HADOOP","server"),CONFIG.get("HADOOP","port")
USER,KEY = CONFIG.get("CREDENTIALS","user"), CONFIG.get("CREDENTIALS","key_phrase")    


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
        
def log_last_date(file_name, date):
    try:
        f = open(file_name,'w')
        f.write('LAST_DATE=' + str(date.date()))
        f.close()
    except Exception as e:
        print(e)
        
def read_last_date(file_name):
    try:
        f = open(file_name,'r')
        last_dt = f.read().split('=')[1]
        f.close()
        return last_dt
    except Exception as e:
        print(e)

def convert_amount(amount):
    return float(amount / 100)
    

print('Fetching last data dump date...')
LAST_DATE = read_last_date('DATE_LOG.txt')

cursor = get_hadoop_cursor()

sms.print_message('fetch_data','Hadoop')
try:
    cursor.execute(sys_queries.query,[LAST_DATE])
except Exception as e:
    logger.log_error(e)
    print(e)

null_resolution_df = as_pandas(cursor)

try:
    cursor.execute(sys_queries.query2,[LAST_DATE])
except Exception as e:
    logger.log_error(e)
    print(e)

not_null_resolution_df = as_pandas(cursor)

close_cursor(cursor)

sms.print_message('manipulate','from Hadoop')
df = pd.concat([null_resolution_df,not_null_resolution_df],ignore_index=True)

df = df.drop_duplicates(subset=["SUBSET_COLUMNS"])

df.drop_duplicates(inplace=True)

if len(df) > 0:
################################################################################ AWS INTEGRATION SETTINGS #######################################################################

    s3_prefix = CONFIG.get("AWS","s3_key")

    buckets = {
        'bucket_data0' : s3_prefix + '<DATA0_PATH>/data0.csv',
        'bucket_data1' : s3_prefix + '<DATA1_PATH>/data1.csv',
        'bucket_data2' : s3_prefix + '<DATA2_PATH>/data2.csv',
        'bucket_data3' : s3_prefix + '<DATA3_PATH>/data3.csv',
        'bucket_data4' : s3_prefix + '<DATA4_PATH>/data4.csv',
        'bucket_data5' : s3_prefix + f'<DATA5_PATH>/data5-{TODAY}.csv',
        'bucket_data6' : s3_prefix + '<DATA6_PATH>/data6.csv',
        'bucket_data7' : s3_prefix + f'<DATA7_PATH>/data7-{TODAY}.csv'
    }

################################################################################ data0 TABLE MANIPULATION #######################################################################
        
    data0_columns = ["COLUMNS"]

    sms.print_message('fetch_data',buckets['bucket_data0'])
    try:
        data0 = pd.read_csv(aws.get_s3_object(buckets['bucket_data0']),header=None, names=data0_columns, dtype='str')
    except Exception as e:
        logger.log_error(buckets['bucket_data0'] + ':' + str(e))
        print('Error occured, please read the errors log file....')

    sms.print_message('manipulate','Warehouse data0 Table')
    new_data0 = '<<BUSINESS_LOGIC>>'
    new_data0['COLUMN'] = df['COLUMN'].drop_duplicates()
    new_data0['COLUMN'] = new_data0['COLUMN'].str.lstrip('0')

    sms.print_message('merge','Dataframes')
    data0 = data0.append(new_data0)

    sms.print_message('drop')
    data0.drop_duplicates(inplace=True)

    sms.print_message('write','data0.CSV')
    data0.to_csv('data0.csv', index=False, header=False)

    aws.download_s3_object('data0',buckets['bucket_data0'])
    if aws.put_s3_object('data0.csv',buckets['bucket_data0']):
        sms.print_message('upload','data0.CSV')

    ################################################################################ data1TABLE MANIPULATION #######################################################################

    data1_columns =  ["COLUMNS"]
    sms.print_message('fetch_data',buckets['bucket_data1'])
    try:
        data1= pd.read_csv(aws.get_s3_object(buckets['bucket_data1']),header=None, names=acc_columns, dtype='str')
    except Exception as e:
        logger.log_error(buckets['bucket_data1'] + ':' + str(e))
        print('Error occured, please read the errors log file....')

    sms.print_message('manipulate','Warehouse data1Table')
    new_data1= '<<BUSINESS_LOGIC>>'
    new_data1['id'] = '<<BUSINESS_LOGIC>>'

    sms.print_message('merge','Dataframes')
    data1= data1.append(new_data1)
    sms.print_message('drop')
    data1.drop_duplicates(inplace=True, subset=['COLUMNS'])

    sms.print_message('write','data1.CSV')
    data1.to_csv('data1.csv', index=False, header=False)

    aws.download_s3_object('data1',buckets['bucket_data1'])
    if aws.put_s3_object('data1.csv',buckets['bucket_data1']):
        sms.print_message('upload','data1.CSV')

    ################################################################################ data2 TABLE MANIPULATION #######################################################################

    data2_columns = ["COLUMNS"]
    sms.print_message('fetch_data',buckets['bucket_data2'])
    try:
        data2 = pd.read_csv(aws.get_s3_object(buckets['bucket_data2']),header=None, names=data2_columns, dtype='str')
    except Exception as e:
        logger.log_error(buckets['bucket_data2'] + ':' + str(e))
        print('Error occured, please read the errors log file....')

    sms.print_message('manipulate','Warehouse data2 Table')
    new_data2 = '<<BUSINESS_LOGIC>>'
    new_data2['id'] = '<<BUSINESS_LOGIC>>'

    sms.print_message('merge','Dataframes')
    data2 = data2.append(new_data2)
    sms.print_message('drop')
    data2.drop_duplicates(inplace=True, subset=['COLUMN'])

    sms.print_message('write','data2.CSV')
    data2.to_csv('data2.csv', index=False, header=False)

    aws.download_s3_object('data2',buckets['bucket_data2'])
    if aws.put_s3_object('data2.csv',buckets['bucket_data2']):
        sms.print_message('upload','data2.CSV')


    ################################################################################ data3 TABLE MANIPULATION #######################################################################

    data3_columns = ['COLUMN']
    sms.print_message('fetch_data',buckets['bucket_data3'])
    try:
        data3 = pd.read_csv(aws.get_s3_object(buckets['bucket_data3']),header=None, names=data3_columns, dtype='str', parse_dates=['TIMESTAMP'])
    except Exception as e:
        logger.log_error(buckets['bucket_data3'] + ':' + str(e))
        print('Error occured, please read the errors log file....')

    sms.print_message('manipulate','Warehouse data3 Table')
    new_data3 = df[['timestamp']].drop_duplicates()
    new_data3['hour'] = new_data3.timestamp.dt.hour
    new_data3['day'] = new_data3.timestamp.dt.day
    new_data3['month'] = new_data3.timestamp.dt.month
    new_data3['year'] = new_data3.timestamp.dt.year
    new_data3['week'] = new_data3.timestamp.dt.week
    new_data3['dayofweek'] = new_data3.timestamp.dt.weekday + 1

    sms.print_message('merge','Dataframes')
    data3 = data3.append(new_data3)
    sms.print_message('drop')
    data3.drop_duplicates(inplace=True, subset=['timestamp'])
    sms.print_message('write','data3.CSV')
    data3.to_csv('data3.csv', index=False, header=False)

    aws.download_s3_object('data3',buckets['bucket_data3'])
    if aws.put_s3_object('data3.csv',buckets['bucket_data3']):
        sms.print_message('upload','data3.CSV')

    ################################################################################ data4 TABLE MANIPULATION #######################################################################

    data4_columns = ['COLUMN']
    sms.print_message('fetch_data',buckets['bucket_data4'])
    try:
        data4 = pd.read_csv(aws.get_s3_object(buckets['bucket_data4']),parse_dates=['timestamp'],header=None, names=data4_columns,dtype='str')
    except Exception as e:
        logger.log_error(buckets['bucket_data4'] + ':' + str(e))
        print('Error occured, please read the errors log file....')

    sms.print_message('manipulate','Warehouse data4 Table')
    data4_list = list(df.loc[df["COLUMN"].notna()]["COLUMN"])
    my_list = map(lambda x: x.split("||"),data4_list)
    flat_list = [item for sublist in my_list for item in sublist]
    new_data4 = pd.DataFrame(flat_list,columns=['column']).drop_duplicates()
    new_data4['last_data3_seen'] = TODAY

    sms.print_message('merge','Dataframes')
    data4 = data4.append(new_data4)
    sms.print_message('drop')
    data4 = data4.sort_values('last_data3_seen', ascending=True).drop_duplicates(subset=['COLUMN'], keep='first')

    sms.print_message('write','data4.CSV')
    data4.to_csv('data4.csv', index=False, header=False)

    aws.download_s3_object('data4',buckets['bucket_data4'])
    if aws.put_s3_object('data4.csv',buckets['bucket_data4']):
        sms.print_message('upload','data4.CSV')
    
    ################################################################################ data5_data4 TABLE MANIPULATION #######################################################################
    
    data5_data4_columns = ['COLUMN']
    data6_data4_key = f'data5_data4-{TODAY}.csv'

    sms.print_message('manipulate','Warehouse data5_data4 Table')
    #Links the data6 with the data4 creating data5_data4 table
    global link
    link = []
    for i, v in df.loc[df['COLUMN'].notna()].iterrows():
        tenant_data4 = v['COLUMN'].split('||')
        for r in tenant_data4:
            link.append((v['COLUMN'],r))

    data5_data4 = pd.DataFrame(data=link, columns=data5_data4_columns)

    sms.print_message('drop')
    data5_data4.drop_duplicates(inplace=True)

    sms.print_message('write',data6_data4_key)
    data5_data4.to_csv(data6_data4_key, index=False, header=False)

    aws.download_s3_object('data5_data4',buckets['bucket_data5'])
    if aws.put_s3_object(data6_data4_key,buckets['bucket_data5']):
        sms.print_message('upload',data6_data4_key)

    
    ################################################################################ data6 TABLE MANIPULATION #######################################################################
    data6_columns = ['COLUMN']
    data6_key = 'data6.csv'

    sms.print_message('fetch_data',buckets['bucket_data6'])
    try:
        data6 = pd.read_csv(aws.get_s3_object(buckets['bucket_data6']),parse_dates=['timestamp'],header=None, names=data6_columns)
    except Exception as e:
        logger.log_error(buckets['bucket_data6'] + ':' + str(e))
        print('Error occured, please read the errors log file....')

    sms.print_message('manipulate','Warehouse data6 Table')
    new_data6 = '<<BUSINESS_LOGIC>>'
    new_data6['data2_id'] = '<<BUSINESS_LOGIC>>'
    new_data6['acc_id'] = '<<BUSINESS_LOGIC>>'
    new_data6['COLUMN'] = '<<BUSINESS_LOGIC>>'

    sms.print_message('merge','Dataframes')
    data6 = data6.append(new_data6)

    sms.print_message('drop')
    data6.drop_duplicates(inplace=True, subset=['data5_id'])
    
    sms.print_message('write',data6_key)
    data6.to_csv(data6_key, index=False, header=False)

    aws.download_s3_object('data6',buckets['bucket_data6'])
    if aws.put_s3_object(data6_key,buckets['bucket_data6']):
        sms.print_message('upload',data6_key)


    sms.print_message('log', 'LAST_DATE_UPDATE')
    log_last_date('DATE_LOG.txt', data6['timestamp'].max())
    
    ################################################################################ data7 TABLE MANIPULATION #######################################################################
    
    data7_key = f'data7-{TODAY}.csv'
    sys_last_cases_dt = read_last_date('SYS_DATE_LOG.txt')
    sys = tc.sys_dataframe()
    if len(sys) > 0:
        sys['COLUMN'] = sys['COLUMN'].map(lambda x: re.sub(r'^[0]{1,}',"",str(x)))
        sys['COLUMN'] = sys['COLUMN'].map(lambda x: re.sub(r'^[0]{1,}',"",str(x)))
        sys['COLUMN'] = sys['COLUMN'].astype(str) + '-' + sys['COLUMN']
        sys['COLUMN'] = sys['COLUMN'].astype(str) + '-' + sys['COLUMN']

        new_cases = sys.loc[sys['COLUMN'] > datedata3.datedata3.strpdata3(sys_last_cases_dt, '%Y-%m-%d').date()]

        data7_table = data6.merge(new_cases, left_on =['COLUMNS'],
                          right_on=['COLUMNS'], how='inner')
    
        ######### looking for <<BUSINESS_LOGIC>> ###########
        cursor = get_hadoop_cursor()
        found_sys_cases = []
        for v in new_cases.itertuples():
            try:
                parameters = [int(v[3]),int(v[2]),int(v[6]) * 100,int(v[9]),int(v[10])]
                cursor.execute(sys_queries_queries.search_sys_case, parameters)
                result = cursor.fetchone()
                if result is not None:
                    found_sys_cases.append(result)
            except Exception as e:
                logger.log_error(e)
                print('Error occured, please read the errors log file....')
        cursor.close()       

        columns = ['COLUMN']

        DATA7_NON_DETECT = pd.DataFrame(data=found_sys_cases, columns = columns)
        DATA7_NON_DETECT['COLUMN'] = DATA7_NON_DETECT['COLUMN'].astype(float)
        DATA7_NON_DETECT['COLUMN'] = DATA7_NON_DETECT['COLUMN'].apply(lambda x : convert_amount(x))          
        DATA7_NON_DETECT['COLUMN'] = DATA7_NON_DETECT['COLUMN'].astype(str) + '-'+ DATA7_NON_DETECT['COLUMN'].astype(str)
        DATA7_NON_DETECT['COLUMN'] = DATA7_NON_DETECT['COLUMN'].astype(str) + '-' + DATA7_NON_DETECT['COLUMN'].astype(str)
        DATA7_NON_DETECT = DATA7_NON_DETECT[data6_columns]
    

        DATA7_NON_DETECT = DATA7_NON_DETECT.merge(new_cases, right_on =['COLUMNS']
                            ,left_on=['COLUMNS'], how='inner')
                           
        if len(data7_table) > 0:
            if len(DATA7_NON_DETECT) > 0:
                data7_table = data7_table.append(DATA7_NON_DETECT)
                data7_table.drop_duplicates(inplace=True, subset=['COLUMNS'])

            sms.print_message('write',data7_key)
            data7_table = data7_table.drop(columns=['COLUMNS'])
            data7_table.to_csv(data7_key, index=False, header=False)
            if aws.put_s3_object(data7_key,buckets['bucket_data7']):
                sms.print_message('upload',data7_key)

            sms.print_message('log', 'sys_LAST_DATE_UPDATE')
            log_last_date('sys_log.txt',new_data6['timestamp'].max())
    
    else:
        print('sys fetched data length is 0...')   

else:
   print('Hadoop fetched data length is 0...')

print('Pipeline execution finished....')