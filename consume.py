#!/usr/bin/python3

import pika, sys, os

import logging
import time
import pika
import os
import json
import time
import zipfile

from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

from pika.exchange_type import ExchangeType

# Lookup environment variables
load_dotenv()

RABBIT_USER_ENV_VAR = os.getenv('RABBIT_USER')
RABBIT_PASS_ENV_VAR = os.getenv('RABBIT_PASS')
MINIO_ENDPOINT_VAR = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY_VAR = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY_VAR = os.getenv('MINIO_SECRET_KEY')

RABBIT_SERVICE = 'rabbitmq'
EXCHANGE = 'dw'
EXCHANGE_TYPE = ExchangeType.direct
QUEUE = 'unpacker-queue'
FORMATTER_QUEUE = 'formatter-queue'
ROUTING_KEY = 'unpacker-queue'

BUCKET_NAME = "input"
UNPACKED_BUCKET_NAME = "unpacked"
DOWNLOADED_FILE_NAME = "temp.zip"


# Create a client with the MinIO server playground, its access key and secret key.
client = Minio(
    endpoint=MINIO_ENDPOINT_VAR,
    access_key=MINIO_ACCESS_KEY_VAR,
    secret_key=MINIO_SECRET_KEY_VAR,
    secure=False
)

# create logger
logger = logging.getLogger('simple_example')
logger.setLevel(logging.INFO)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)


def main():
    logger.info('opening connection')

    credentials = pika.PlainCredentials(RABBIT_USER_ENV_VAR, RABBIT_PASS_ENV_VAR)
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE)
    channel.basic_consume(queue=QUEUE,
                          auto_ack=True,
                          on_message_callback=callback)

    channel.start_consuming()

def callback(ch, method, properties, body):
    logger.info(" [x] Received %r" % body)

    json_body = json.loads(body)

    #check new data type
    # event = json.dumps(body.decode())
    records = []

    # process each record in the event
    if 'EventName' in json_body and json_body['EventName'] == 's3:ObjectCreated:Put':
        for record in json_body['Records']:
            BUCKET_NAME = record['s3']['bucket']['name']
            key = record['s3']['object']['key']

            try:
                # download the zip file
                client.fget_object(BUCKET_NAME, key, DOWNLOADED_FILE_NAME)
                logger.info('downloaded %s, from %s', key, BUCKET_NAME)

                # unzip each file in memory
                unzipped = []

                # name the folder using now time
                folder_name = time.strftime("%Y%m%d-%H%M%S")


                input_zip=zipfile.ZipFile(DOWNLOADED_FILE_NAME)
                
                # create a counter to use as the id of the file in the json object
                i = 0
                for file_name in input_zip.namelist():
                    logger.info('file downloaded %s', file_name)

                    # post file to minio in its own folder
                    client.fput_object(UNPACKED_BUCKET_NAME, folder_name + '/' + file_name, file_name)

                    i += 1
                    unzipped.append({'id': i, 'file': folder_name + '/' + file_name, 'bucket': UNPACKED_BUCKET_NAME})


                # with zipfile.ZipFile(DOWNLOADED_FILE_NAME) as archive:
                #     i = 0
                #     archive.extractall()
                    
                #     for file in archive.namelist():


                #         # post file to minio in its own folder
                #         client.fput_object(UNPACKED_BUCKET_NAME, folder_name + '/' + file, file)

                #         i += 1
                #         unzipped.append({'id': i, 'file': folder_name + '/' + file, 'bucket': UNPACKED_BUCKET_NAME})

                        

                # update event
                record['unzipped'] = unzipped
                records.append(record)
                    
            except Exception as e:
                logger.info('error processing key %s, from bucket %s, error message: %s', key, BUCKET_NAME, e)


        publish_message(records)

def publish_message(message): 
    logger.info('received message %s', message) 

    try:                                                                     
        credentials = pika.PlainCredentials(RABBIT_USER_ENV_VAR, RABBIT_PASS_ENV_VAR)
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', credentials=credentials))
                                                                                                            
        channel = connection.channel()                                                                      
        channel.exchange_declare(exchange='dw', exchange_type='direct')              
        channel.basic_publish(exchange='dw', routing_key='formatter-queue', body=json.dumps(message))   

        logger.info('sent message')                                                                     
    except Exception as e:
                logger.info('error publishing message, error message: ', e)

    connection.close() 



if __name__ == '__main__':
    try:
        main()
    
    except KeyboardInterrupt:
        logger.info('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
