#!/usr/bin/python3

import pika, sys, os

import functools
# import logging
import time
import pika
import os
import json

from dotenv import load_dotenv

from pika.exchange_type import ExchangeType

# LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
#               '-35s %(lineno) -5d: %(message)s')
# LOGGER = logging.getLogger(__name__)

# Lodookup environment variables
load_dotenv()

# Lodookup environment variables
RABBIT_USER_ENV_VAR = os.getenv('RABBIT_USER')
RABBIT_PASS_ENV_VAR = os.getenv('RABBIT_PASS')

RABBIT_SERVICE = 'rabbitmq'

EXCHANGE = 'dw'
EXCHANGE_TYPE = ExchangeType.direct
QUEUE = 'unpacker-queue'
FORMATTER_QUEUE = 'formatter-queue'
ROUTING_KEY = 'unpacker-queue'


def main():
    print('opening connection')

    credentials = pika.PlainCredentials(RABBIT_USER_ENV_VAR, RABBIT_PASS_ENV_VAR)
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE)
    channel.basic_consume(queue=QUEUE,
                          auto_ack=True,
                          on_message_callback=callback)

    print('listening for notifications..')
    channel.start_consuming()

def callback(ch, method, properties, body):
    # callback for message
    # self.acknowledge_message(basic_deliver.delivery_tag)

    print(" [x] Received %r" % body)

    # publish_message('hello world')


if __name__ == '__main__':
    try:
        print('calling main')
        main()
    
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
