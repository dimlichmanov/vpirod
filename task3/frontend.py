import pika
import threading


class Frontend:
    def __init__(self):
        self.filename = None
        self.num_parts = None
        self.connection_client = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel_client = self.connection_client.channel()

        self.connection_backend = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel_backend = self.connection_backend.channel()

        self.queue_response = None
        self.properties = None
        self.response = None

        result = self.channel_client.queue_declare(queue='frontend_queue', exclusive=False)  # Queue for queries to frontend
        self.query_queue = result.method.queue
        result = self.channel_backend.queue_declare(queue='back_to_front_queue', exclusive=True)  # Queue for backend-frontend communication
        self.callback_queue = result.method.queue

        self.channel_client.basic_consume(
            queue=self.query_queue,
            on_message_callback=self.callback_actions_client,
            auto_ack=True)

        self.channel_backend.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.callback_actions_backend,
            auto_ack=True)

    def get_minimaps(self):
        self.start_consumer_client()
        if self.response:
            message_send = self.response
            self.channel_backend.basic_publish(exchange='', routing_key='backend_coord_queue', body=message_send)
            self.response = None
        for i in range(self.num_parts):
            message_resp = self.start_consumer_backend()
            self.channel_backend.basic_publish(exchange='', routing_key=self.queue_response, body=message_resp)
            self.response = None

    def callback_actions_client(self, ch, method, properties, body):
        message_rec = body.decode("utf-8")
        self.queue_response = properties.reply_to
        self.response = message_rec

    def callback_actions_backend(self, ch, method, properties, body):
        message_rec = body
        self.response = message_rec

    def start_consumer_client(self):
        while self.response is None:
            self.connection_client.process_data_events()
        return self.response

    def start_consumer_backend(self):
        while self.response is None:
            self.connection_backend.process_data_events()
        return self.response


if __name__ == '__main__':
    a = Frontend()
    a.get_minimaps()
