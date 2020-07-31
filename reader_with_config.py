#!/usr/bin/python

import time
import os
import sys
import getopt
import pika
import json
import signal

class squid_to_rmq_injector:

    type_application = "application"
    type_application_json = "application/json"

    type_img = ["image/gif",
                "image/png",
                "image/jpeg",
                "image/webp",]

    type_media =["video/mp4",
                "video/mpeg"]

    advert_url_keys = ['daca_images', 'googlesyndication']

    def __init__(self, config: dict):
        
        self._file_name = config['file_name']
        self._rabbit_mq_host = config['host']
        self._queue_name = config['queue_name']
        self._tail_log = config['tail_file']
        self._read_full_log = config['read_full_log']
        self._clear_log_on_finish = config['clear_log_on_finish'] 
        self._config = config

        self._rmq_connection = self.init_rabbit_mq_connection()
        
    def init_rabbit_mq_connection(self):
        credentials = pika.PlainCredentials('rabbit', 'rabbit') # user, password
        parameters = pika.ConnectionParameters(self._rabbit_mq_host, 5672, '/', credentials)
        return pika.BlockingConnection(parameters)

    def send_msg(self, msg):
        channel = self._rmq_connection.channel()
        channel.queue_declare(queue=self._queue_name , durable=True,
                            arguments={'x-message-ttl': 86400000, 'x-max-length': 5242880})

        channel.basic_publish(exchange='',
                            routing_key=self._queue_name,
                            body=msg)

    def start(self):
        # Set the filename and open the file
        if self._read_full_log:
            self.read_whole_log()

            if self._clear_log_on_finish:
                self.clear_the_log_file()

        if self._tail_log:
            self.tail_log()

    def tail_log(self):
        print("Open file: ", self._file_name)
        file = open(self._file_name, 'r')

        # Find the size of the file and move to the end
        st_results = os.stat(self._file_name)
        st_size = st_results[6]
        file.seek(st_size)
        print("Initial File Size: ", str(st_size))

        while 1:
            where = file.tell()
            line = file.readline()
            if not line:
                time.sleep(5)
                file.seek(where)
            else:
                if self.is_message_type(line if line else None, "image"):
                    print("Push: ", line if line else None)  
                    self.send_msg(line if line else 'NONE')

    def is_message_type(self, log_line, message_string):
        if log_line is None:
            return False            
        log_line = str(log_line)  # only cast once
        parts = log_line.split("|||") # only split once
        if len(parts) > 4: 
            if message_string in parts[4]:
                return True
        return False 


    def read_whole_log(self):
        print("sending log lines to queue")
        with open(self._file_name, 'r') as f:
            for line in f:
                self.send_msg(line)

    def clear_the_log_file(self):
        print("Clearing log file for read")
        with open(self._file_name, 'w'):
            pass





if __name__ == "__main__":
    with open('reader_config.json', 'r') as f:
        config = json.load(f)
        stri = squid_to_rmq_injector(config)
        stri.start()

        
