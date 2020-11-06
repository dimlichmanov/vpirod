import pika
import threading
import uuid


class BackendCoord:
    def __init__(self):
        self.filename = None
        self.num_parts = None
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.response = None
        self.corr_id = None

        result = self.channel.queue_declare(queue='backend_coord_queue', exclusive=True)  # Queue for queries to backend
        self.query_queue = result.method.queue

        result = self.channel.queue_declare(queue='workers_to_coord', exclusive=True)  # Queue for queries to backend
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.query_queue,
            on_message_callback=self.callback_actions,
            auto_ack=True)

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.callback_actions,
            auto_ack=True)

    def get_chat(self):
        message_send = '{}_{}'.format(self.filename, self.num_parts)
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='', routing_key='backend_coord_queue', properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ), body=message_send)
        resp = self.start_consumer()
        print(resp)
        self.response = None

    def callback_actions(self, ch, method, properties, body):
        message_rec = body.decode("utf-8").split('_')
        self.filename = str(message_rec[0])
        self.num_parts = str(message_rec[1])


    def start_consumer(self):
        # Sleep on input queue
        while self.response is None:
            self.connection.process_data_events()
        return self.response


if __name__ == '__main__':
    a = BackendCoord()
