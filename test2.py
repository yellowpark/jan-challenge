import functools
import logging
import time
import pika
import os
import json

from dotenv import load_dotenv

from pika.exchange_type import ExchangeType


EXCHANGE = 'dw'
EXCHANGE_TYPE = ExchangeType.topic
QUEUE = 'unpacker-queue'
FORMATTER_QUEUE = 'formatter-queue'
ROUTING_KEY = 'example.text'

# Lodookup environment variables
load_dotenv()

# Lodookup environment variables
RABBIT_USER_ENV_VAR = os.getenv('RABBIT_USER')
RABBIT_PASS_ENV_VAR = os.getenv('RABBIT_PASS')

RABBIT_SERVICE = 'rabbitmq'

message = 'hello, world'
try:
    credentials = pika.PlainCredentials(RABBIT_USER_ENV_VAR, RABBIT_PASS_ENV_VAR)
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', credentials=credentials))
    
    channel = connection.channel()

    # channel.basic_publish(EXCHANGE, FORMATTER_QUEUE, body=message)
    channel.basic_publish(EXCHANGE,
        FORMATTER_QUEUE,
        json.dumps(message),
        pika.BasicProperties(content_type='text/json',
        delivery_mode=pika.DeliveryMode.Transient))
    
    print(" [x] Sent %r" % message)
    connection.close()
except Exception as e:
    print(f'failed to publish message to broker - {message} - {e}')