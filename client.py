import pika
import uuid


class NY_query(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        self.response = None
        self.corr_id = None

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)  # Queue for replies
        self.callback_queue = result.method.queue # A single queue for two servers

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=str(n))
    def get_result(self):
        while self.response is None:
            self.connection.process_data_events()
        return self.response

class Portland_query(object):

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        self.response = None
        self.corr_id = None

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)  # Queue for replies
        self.callback_queue = result.method.queue # A single queue for two servers

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc1_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=str(n))

    def get_result(self):
        while self.response is None:
            self.connection.process_data_events()
        return self.response





ny_client = NY_query()
portland_client = Portland_query()
print('a')
ny_client.call(ord('B'))
print('b')
portland_client.call(ord('B'))
print('c')
response_from_ny = ny_client.get_result()
print('d')
response_from_portland = portland_client.get_result()
print('e')

if int(response_from_ny) > int(response_from_portland):
    print('NY has more roads {} versus {}'.format(int(response_from_ny), int(response_from_portland)))
else:
    print('Portland has more roads {} versus {}'.format(int(response_from_portland), int(response_from_ny)))
