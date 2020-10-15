import sys
import threading
import pika
import time


class ForumClient:

    def __init__(self, nickname, logical_time=0):
        self.logical_time = logical_time
        self.nickname = nickname

        # Initialize "send" connection on a master thread
        self.connection_send = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel_send = self.connection_send.channel()

        # Configure "send" channel
        self.channel_send.exchange_declare(exchange='send_to_server', exchange_type='fanout')

        # Initialize a listening thread
        consumer_thread = threading.Thread(target=self.start_consumer)
        consumer_thread.start()


    def update_time(self, new_time):
        # Update logical time through a critical section
        #update_lock = threading.Lock()
        #update_lock.acquire()
        if self.logical_time > new_time:
            self.logical_time += 1
        else:
            self.logical_time = new_time + 1
        #update_lock.release()

    def send(self, text_message, response_to):
        # Update logical time and send a message
        #self.update_time(-1)
        message = '{}_{}_{}_{}'.format(self.nickname, text_message, response_to, self.logical_time)
        self.channel_send.basic_publish(exchange='send_to_server', routing_key='', body=message)

    def callback_actions(self, ch, method, properties, body):
        # Update local logic time and sleep again
        print(body)
        received_time = int(body.split('_')[-1])
        self.update_time(received_time)
        # TODO check if start_consuming checks already present messages

    def consume(self):
        # Set consumer parameters and sleep on a reading queue
        self.channel_receive.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback_actions, auto_ack=True)
        time.sleep(15)
        self.channel_receive.start_consuming()

    def start_consumer(self):
        # Pika is not safe with a shared Connection - initialize new connection in new thread
        self.connection_receive = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel_receive = self.connection_receive.channel()

        # Configure "receive" channel - unique queue for each client
        result = self.channel_receive.queue_declare(queue=self.nickname, exclusive=True)
        self.queue_name = result.method.queue
        self.channel_receive.exchange_declare(exchange='send_from_server', exchange_type='fanout')
        self.channel_receive.queue_bind(exchange='send_from_server', queue=self.queue_name)
        self.consume()


if __name__ == '__main__':
    client = ForumClient(nickname='Dmitry', logical_time=0)
    client.send('Privet', 0)
