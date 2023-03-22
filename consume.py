#!/usr/bin/python3

import pika, sys, os

import functools
import logging
import time
import pika
import os
import json

from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

from pika.exchange_type import ExchangeType

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

# Lodookup environment variables
load_dotenv()

# Lodookup environment variables
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


# Create a client with the MinIO server playground, its access key
# and secret key.
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

    publish_message(body)

def publish_message(message): 
    logger.info('received message')                                                                          
    credentials = pika.PlainCredentials(RABBIT_USER_ENV_VAR, RABBIT_PASS_ENV_VAR)
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', credentials=credentials))
                                                                                                        
    channel = connection.channel()                                                                      
    channel.exchange_declare(exchange='dw', exchange_type='direct')              
    channel.basic_publish(exchange='dw', routing_key='formatter-queue', body=message)   

    json_body = json.loads(message)

    #check new data type
    # logging.info(type(json_body))


    # logging.info('received: {json_body}')
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
                logger.info('downloaded {key} from {BUCKET_NAME}')
                
                logger.info('Unzipped file')

                # # unzip each file in memory
                unzipped = []

                # with zipfile.ZipFile(DOWNLOADED_FILE_NAME) as archive:
                #     archive.extractall()
                #     for file in archive.namelist():
                #         print(file)

                #         # post file to minio in its own folder
                #         client.fput_object(UNPACKED_BUCKET_NAME, 'newfolder/' + file, file)

                #         unzipped.append({'id': str(uuid.uuid4()), 'key': 'newfolder/' + file, 'bucket': UNPACKED_BUCKET_NAME})

                # update event
                # record['unzipped'] = unzipped
                # records.append(record)
                    
            except Exception as e:
                logger.info('error processing key [{key}] from bucket [{BUCKET_NAME}] - {e}')

    logger.info('sent message')                                                                     

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
