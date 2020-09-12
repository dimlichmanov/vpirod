import pika
import uuid


class FibonacciRpcClient(object):

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
        while self.response is None:
            self.connection.process_data_events()
        return int(self.response)

class FibonacciRpcClient1(object):

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
        while self.response is None:
            self.connection.process_data_events()
        return int(self.response)





fibonacci_rpc1 = FibonacciRpcClient()

print(" [x] Requesting fib(30)")
response = fibonacci_rpc1.call(30)
print(" [.] Got %r" % response)

fibonacci_rpc2 = FibonacciRpcClient1()

print(" [x] Requesting fib(30)")
response = fibonacci_rpc2.call(20)
print(" [.] Got %r" % response)
