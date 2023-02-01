import cloud_authorizer
import configparser 
import time
import error_logger as logger

CONFIG = configparser.ConfigParser()
CONFIG.read("config.ini")
ADFS_ACC,ADFS_ROLE, S3_BUCKET, S3_KEY = CONFIG.get("AWS","adfs_account"),CONFIG.get("AWS","adfs_role"), CONFIG.get("AWS","s3_bucket"), CONFIG.get("AWS","s3_key")
CALL_BACKS = 0

try:
    aws_session = cloud_authorizer.adfs_authenticate(adfs_account=ADFS_ACC, adfs_role=ADFS_ROLE)
except Exception as e:
        logger.log_error(e)
        print('Not Possible to start AWS Sessio ... Please read the log file....')

s3_client = aws_session.client('s3')

def get_s3_object(key=S3_KEY):
    try:
        print(f'Retriving data from AWS  {S3_KEY}...')
        bucket_object = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    except Exception as e:
        logger.log_error(key + ':' + str(e))
        print('Not Possible to get file from AWS: ' + key + "please read the log file....")
    return bucket_object['Body']

def put_s3_object(file,key=S3_KEY):
    try:
        print(f'Uploading file To AWS Bucket {key}...')
        s3_client.upload_file(file, S3_BUCKET, key)
        print(f'{key} Uploaded successfully...')
        #CALL_BACKS = 0
        return True
    except Exception as e:
        logger.log_error(key + ':' + str(e))
        print('Not Possible to upload file ' + file + " Another attempt will be made...please read the log file....")
        print('\nCallback to push object again...')
        pushing_upload(file,key)
        
def pushing_upload(file, key=S3_KEY):
    #if CALL_BACKS < 3:
        time.sleep(30)
        put_s3_object(file,key)
        #CALL_BACKS = CALL_BACKS + 1

def download_s3_object(file,key=S3_KEY):
    try:
        print(f'Downloading backup file from AWS Bucket {key}...')
        s3_resource = aws_session.resource('s3')
        s3_resource.Bucket(S3_BUCKET).download_file(key, f'{file}-s3-dataframe-backup.csv')
        print(f'Download succesful...')
    except Exception as e:
        logger.log_error(key + ':' + str(e))
        print('Not Possible to Download backup from ' + key + "please read the log file....")
        