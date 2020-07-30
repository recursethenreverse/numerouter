#!/usr/bin/python

import time
import os
import sys
import getopt
import pika
import json



def read_log(file_name, rabbit_mq_host, queue_name, tail_log, read_full_log, clear_log_on_finish):
    # Set the filename and open the file
    if read_full_log:
        read_whole_log(file_name, rabbit_mq_host, queue_name)
        if clear_log_on_finish:
            clear_the_log_file(file_name)

    if tail_log:
        print("Open file: ", file_name)
        file = open(file_name, 'r')

        # Find the size of the file and move to the end
        st_results = os.stat(file_name)
        st_size = st_results[6]
        file.seek(st_size)
        print("Initial File Size: ", str(st_size))

        while 1:
            where = file.tell()
            line = file.readline()
            if not line:
                time.sleep(1)
                file.seek(where)
            else:
                print(line if line else None)  # already has newline
                send_msg(line if line else 'NONE', rabbit_mq_host, queue_name)


def read_whole_log(file_name, rabbit_mq_host, queue_name):
    print("sending log lines to queue")
    with open(file_name, 'r') as f:
        for line in f:
            send_msg(line, rabbit_mq_host, queue_name)

def clear_the_log_file(file_name):
    if clear_log:
        print("Clearing log file for read")
        with open(file_name, 'w'):
            pass


def send_msg(msg, host, queue_name):
    credentials = pika.PlainCredentials('rabbit', 'rabbit') # user, password
    parameters = pika.ConnectionParameters(host, 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True,
                          arguments={'x-message-ttl': 86400000, 'x-max-length': 5242880})

    channel.basic_publish(exchange='',
                          routing_key=queue_name,
                          body=msg)


if __name__ == "__main__":
    with open('/usr/local/reader_config.json', 'r') as f:
        config = json.load(f)
        print(config)
        read_log(config['file_name'], 
                    config['host'], 
                    config['queue_name'], 
                    config['tail_file'], 
                    config['read_full_log'],
                    config['clear_log_on_finish'] )
        
