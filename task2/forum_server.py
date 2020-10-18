import pika


class ForumServer:
    def __init__(self, logical_time=0):
        self.logical_time = logical_time
        self.message_counter = 1

        # Initialize connections
        self.connection_send = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.connection_receive = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel_receive = self.connection_receive.channel()
        self.channel_send = self.connection_send.channel()

        # Configure "send" channel
        self.channel_send.exchange_declare(exchange='send_from_server', exchange_type='fanout')
        self.channel_receive.exchange_declare(exchange='send_to_server', exchange_type='fanout')

        # Configure "receive" channel - an only queue from clients
        result = self.channel_receive.queue_declare(queue='server', exclusive=True)
        self.queue_name = result.method.queue
        self.channel_receive.queue_bind(exchange='send_to_server', queue=self.queue_name)

        print('#0 - Welcome to chat')

        # Sleeping on receive channel
        self.start_consumer()

    def update_time(self, new_time):
        if self.logical_time > new_time:
            self.logical_time += 1
        else:
            self.logical_time = new_time + 1

    def callback_actions(self, ch, method, properties, body):
        # Split a message, retrieve a logical time, send a client-friendly message to clients
        elements = body.decode("utf-8").split('_')
        new_logical_time = int(elements[-1])
        self.update_time(new_logical_time)
        print(self.logical_time)
        posting = '#{} - User {} replied to message {} : {} (time : {})'.format(self.message_counter, *elements[:-1], self.logical_time)
        print(posting)
        self.message_counter += 1
        message = posting + '_' + str(new_logical_time)
        self.channel_send.basic_publish(exchange='send_from_server', routing_key='', body=message)

    def start_consumer(self):
        self.channel_receive.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback_actions, auto_ack=True)
        self.channel_receive.start_consuming()


if __name__ == '__main__':
    server = ForumServer()
