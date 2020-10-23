import threading
import pika
import time


class ForumClient:

    def __init__(self, nickname, logical_time=0):
        self.logical_time = logical_time
        self.nickname = nickname
        self.update_lock = threading.Lock()
        self.response = None

        self.connection_receive = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel_receive = self.connection_receive.channel()
        result = self.channel_receive.queue_declare(queue=self.nickname, exclusive=True)
        self.queue_name = result.method.queue
        self.channel_receive.exchange_declare(exchange='send_from_server', exchange_type='fanout')
        self.channel_receive.queue_bind(exchange='send_from_server', queue=self.queue_name)
        self.channel_receive.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.callback_actions,
            auto_ack=True)

        self.connection_send = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel_send = self.connection_send.channel()
        self.channel_send.exchange_declare(exchange='send_to_server', exchange_type='fanout')

    def update_time(self, new_time):
        # Update logical time through a critical section

        self.update_lock.acquire()
        if self.logical_time > new_time:
            self.logical_time += 1
        else:
            self.logical_time = new_time + 1
        self.update_lock.release()

    def send(self, text_message, response_to):
        # Update logical time and send a message
        message_send = '{}_{}_{}_{}'.format(self.nickname, text_message, response_to, self.logical_time)
        self.channel_send.basic_publish(exchange='send_to_server', routing_key='', properties=pika.BasicProperties(reply_to=self.queue_name), body=message_send)

    def get_chat(self):
        message_send = '{}_{}'.format(self.nickname, self.logical_time)
        self.channel_send.basic_publish(exchange='send_to_server', routing_key='', body=message_send)
        resp = self.start_consumer()
        print(resp)
        self.response = None

    def callback_actions(self, ch, method, properties, body):
        # Update local logic time and sleep again
        message_rec = body.decode("utf-8").split('_')
        print(message_rec)
        # received_time = int(message_rec[-1])
        # self.update_time(received_time)
        self.response = str(message_rec[0])

    def start_consumer(self):
        # Set consumer parameters and sleep on a reading queue
        while self.response is None:
            self.connection_receive.process_data_events()
        return self.response


if __name__ == '__main__':
    initial = str(input("Enter yor nickname and logical time: "))
    nick = str(initial.split('_')[0])
    log_time = int(initial.split('_')[1])

    client = ForumClient(nickname=nick, logical_time=log_time)

    message = str(input("Enter yor message: "))
    parts = message.split('_')
    if len(parts) == 2:
        text = str(message.split('_')[0])
        response = int(message.split('_')[1])
    while message:
        if len(parts) == 2:
            client.send(message, response)
        else:
            client.get_chat()
        time.sleep(3)  # ONLY FOR STDIN
        message = str(input("Enter yor message: "))
        parts = message.split('_')
        if len(parts) == 2:
            text = str(message.split('_')[0])
            response = int(message.split('_')[1])
