#!/usr/bin/env python
import pika, sys, os
import functools
import logging
import time

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='unpacker-queue')

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
    
    channel.basic_consume(queue='unpacker-queue',
                      auto_ack=True,
                      on_message_callback=on_message)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

def on_message(self, _unused_channel, basic_deliver, properties, body):
    """Invoked by pika when a message is delivered from RabbitMQ. The
    channel is passed for your convenience. The basic_deliver object that
    is passed in carries the exchange, routing key, delivery tag and
    a redelivered flag for the message. The properties passed in is an
    instance of BasicProperties with the message properties and the body
    is the message that was sent.
    :param pika.channel.Channel _unused_channel: The channel object
    :param pika.Spec.Basic.Deliver: basic_deliver method
    :param pika.Spec.BasicProperties: properties
    :param bytes body: The message body
    """
    LOGGER.info('Received message # %s from %s: %s',
                basic_deliver.delivery_tag, properties.app_id, body)

if __name__ == '__main__':
    try:
        main()
    
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
