import sys
import pika
import time
import threading


class ForumServer:
    def __init__(self, logical_time=0):
        self.logical_time = logical_time

        # Initialize connections
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel_receive = connection.channel()
        self.channel_send = connection.channel()

        # Configure "send" channel
        self.channel_send.exchange_declare(exchange='send_from_server', exchange_type='fanout')

        # Configure "receive" channel - an only queue from clients
        result = self.channel_receive.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        self.queue_name.bind(exchange='send_to_server')

    def update_time(self, new_time):
        if self.logical_time > new_time:
            self.logical_time += 1
        else:
            self.logical_time = new_time + 1

    def process_message(self, message):
        # Split a message, retrieve a logical time, send a client-friendly message to clients
        elements = message.split('_')
        new_logical_time = elements[-1]
        self.update_time(new_logical_time)
        posting = 'User {} replied to message {} : {} (time : {})'.format(*elements)
        message = posting + '_' + new_logical_time
        self.send(message)

    def send(self, message):
        self.channel_send.basic_publish(exchange='send_from_server', routing_key='', body=message)
        self.start_consumer()

    def callback_actions(self, ch, method, properties, body):
        self.process_message(body)

    def start_consumer(self):
        self.channel_receive.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback_actions(), auto_ack=True)
        self.channel_send.start_consuming()


if __name__ == '__main__':
    server = ForumServer()
