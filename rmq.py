import pika


class RMQiface:
    def __init__(self, host, queue_name, user, pwd):
        credentials = pika.PlainCredentials(user, pwd)
        parameters = pika.ConnectionParameters(host, 5672, '/', credentials)
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments={'x-message-ttl': 86400000, 'x-max-length': 5242880}
        )

    def write(self, msg):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=msg)

    def read(self):
        method_frame, header_frame, body = self.channel.basic_get(queue=self.queue_name)
        if not method_frame or method_frame.NAME == 'Basic.GetEmpty':  # looks for empty, i.e. end of file
            return None
        else:
            self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        return body.decode()
