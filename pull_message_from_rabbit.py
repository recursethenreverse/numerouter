import pika
import json


def receive_message(host, queue_name):

    #connection setup
    credentials = pika.PlainCredentials('rabbit', 'rabbit')
    parameters = pika.ConnectionParameters(host,
                                        5672,
                                        '/',
                                        credentials)

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, 
                        durable=True,
                        arguments={'x-message-ttl' : 86400000,
                                    'x-max-length': 5242880},
                        )

    # reads only 5 messages - but conserves the connection for each
    # this method of reading destroys the message in the queue whenever it is read
    for i in range(5):
        method_frame, header_frame, body = channel.basic_get(queue = 'adverts')  

        if method_frame.NAME == 'Basic.GetEmpty': #looks for empty, i.e. end of file
            connection.close()
            return
        else:            
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            print(body)

    connection.close()



if __name__ == "__main__":
    receive_message('192.168.1.204', "adverts")