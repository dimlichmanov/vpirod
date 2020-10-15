import sys
import threading
import pika


class ForumClient:
    def __init__(self, nickname, logical_time=0):
        self.logical_time = logical_time
        self.nickname = nickname
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange='send_to_server', exchange_type='fanout')
        # callback queue
        result = self.channel.queue_declare(queue='', exclusive=False)
        self.queue_name = result.method.queue
        consumer_thread = threading.Thread(target=self.start_consumer)
        consumer_thread.start()

    def update_time(self, new_time):
        update_lock = threading.Lock
        update_lock.acquire()
        if self.logical_time > new_time:
            self.logical_time += 1
        else:
            self.logical_time = new_time + 1
        update_lock.release()

    def send(self, text_message, response_to):
        self.update_time(-1)  # check if consuming thread updated a logical time
        message = '{}_{}_{}_{}'.format(self.nickname, text_message, response_to, self.logical_time)
        self.channel.basic_publish(exchange='logs', routing_key='', body=message)

    def callback_actions(self, ch, method, properties, body):
        received_time = int(body.split('_')[-1])
        self.update_time(received_time)
        print(body)
        self.start_consumer()

    def start_consumer(self):
        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback_actions(), auto_ack=True)
        self.channel.start_consuming()


if __name__ == '__main__':
    client = ForumClient(nickname='Dmitry', logical_time=0)
    client.send('Privet', 0)
