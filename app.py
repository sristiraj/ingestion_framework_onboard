import json
import logging 
import boto3
from ingestion_framework.onboard import trigger

logger = logging.getLogger(__file__)

def read_s3(s3_file):
    s3 = boto3.client("s3")
    s3_bucket_index = s3_file.replace("s3://","").find("/")
    s3_bucket = s3_file[5:s3_bucket_index+5]
    s3_key = s3_file[s3_bucket_index+6:]
    obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)            
    data = obj["Body"].read().decode('utf-8') 
    return data  


def read_s3_event_file(bucket_name, object_key):
    #Read s3 event file data in string buffer
    data = read_s3(f"s3://{bucket_name}/{object_key}".format())

    return data


def lambda_handler(event, context):
    logger.info(event)
    logger.info('## INITIATED BY EVENT: ')
    logger.info("Read Event Source")
    print(event)
    eventSource = event.get("Records","unknown")[0].get("eventSource","unknown")
    
    if eventSource=="aws:s3":
        bucket_name = event["Records"][0]["s3"]['bucket']["name"]
        object_key =  event["Records"][0]["s3"]["object"]["key"]
        event_record = read_s3_event_file(bucket_name, object_key)  
        logger.info("Event Record: {}".format(json.dumps(event_record)))

        key_prefix = object_key[:object_key.find("/")]

    trigger(event_record)
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
