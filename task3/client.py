import pika
import threading
import uuid


class Client:
    def __init__(self, filename, num_parts=4):
        self.filename = filename
        self.num_parts = num_parts
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.response = None
        self.corr_id = None

        result = self.channel.queue_declare(queue='', exclusive=True)  # Queue for replies
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.callback_actions,
            auto_ack=True)

    def get_minimaps(self):
        message_send = '{}_{}'.format(self.filename, self.num_parts)
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='', routing_key='frontend_queue', properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ), body=message_send)
        for i in range(self.num_parts):
            resp = self.start_consumer()
            # TODO display result
            print(resp)
            self.response = None

    def callback_actions(self, ch, method, properties, body):
        self.response = body

    def start_consumer(self):
        # Sleep on input queue
        while self.response is None:
            self.connection.process_data_events()
        return self.response


if __name__ == '__main__':
    geojson_full = './admin1-us.geojson'
    a = Client(geojson_full)
    a.get_minimaps()
