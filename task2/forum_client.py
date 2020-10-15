import sys
import threading
import pika


class ForumClient:
    def __init__(self, nickname, logical_time=0):
        self.logical_time = logical_time
        self.nickname = nickname

        # Initialize connections
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel_receive = connection.channel()
        self.channel_send = connection.channel()

        # Configure "send" channel
        self.channel_send.exchange_declare(exchange='send_to_server', exchange_type='fanout')

        # Configure "receive" channel - unique queue for each client
        result = self.channel_receive.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        self.queue_name.bind(exchange='send_from_server')

        # Initialize a listening thread
        consumer_thread = threading.Thread(target=self.start_consumer)
        consumer_thread.start()

    def update_time(self, new_time):
        # Update logical time through a critical section
        update_lock = threading.Lock
        update_lock.acquire()
        if self.logical_time > new_time:
            self.logical_time += 1
        else:
            self.logical_time = new_time + 1
        update_lock.release()

    def send(self, text_message, response_to):
        # Update logical time and send a message
        self.update_time(-1)
        message = '{}_{}_{}_{}'.format(self.nickname, text_message, response_to, self.logical_time)
        self.channel_send.basic_publish(exchange='logs', routing_key='', body=message)
        # TODO user-based sending interface

    def callback_actions(self, ch, method, properties, body):
        # Update local logic time and sleep again
        print(body)
        received_time = int(body.split('_')[-1])
        self.update_time(received_time)
        self.start_consumer()
        # TODO check if start_consuming checks already present messages

    def start_consumer(self):
        # Set consumer parameters and sleep on a reading queue
        self.channel_receive.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback_actions(), auto_ack=True)
        self.channel_receive.start_consuming()


if __name__ == '__main__':
    client = ForumClient(nickname='Dmitry', logical_time=0)
    #client.send('Privet', 0)
