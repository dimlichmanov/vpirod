import pika


class BackendCoord:
    def __init__(self, num_workers):
        self.filename = None
        self.hor_parts = None
        self.vert_parts = None
        self.num_parts = None
        self.hor_chunk = 60
        self.vert_chunk = 30
        self.num_workers = num_workers

        self.connection_frontend = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel_frontend = self.connection_frontend.channel()

        self.connection_backend = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel_backend = self.connection_backend.channel()

        self.properties = None
        self.response = None

        self.borders = [-125, 21]

        result = self.channel_frontend.queue_declare(queue='backend_coord_queue', exclusive=False)  # Queue for queries to frontend
        self.query_queue = result.method.queue
        result = self.channel_backend.queue_declare(queue='workers_to_coord_queue', exclusive=True)  # Queue for backend-frontend communication
        self.callback_queue = result.method.queue

        self.channel_frontend.basic_consume(
            queue=self.query_queue,
            on_message_callback=self.callback_actions_frontend,
            auto_ack=True)

        self.channel_backend.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.callback_actions_backend,
            auto_ack=True)

    def get_minimaps(self):
        self.start_consumer_frontend()
        if self.response:
            self.response = None
            message = []
            for i in range(self.num_parts):
                coord1 = self.borders[0] + self.hor_chunk * (i % self.hor_parts)
                coord2 = self.borders[1] + self.vert_chunk * (i // self.hor_parts)
                coord3 = coord1
                coord4 = self.borders[1] + self.vert_chunk * ((i // self.hor_parts) + 1)
                coord5 = self.borders[0] + self.hor_chunk * ((i % self.hor_parts) + 1)
                coord6 = coord4
                coord7 = coord5
                coord8 = coord2
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, coord1, coord2, coord3, coord4, coord5, coord6, coord7, coord8))
            for i in range(self.num_parts):
                self.channel_backend.basic_publish(exchange='', routing_key='worker_{}'.format(i % self.num_workers), body=message[i])

        for i in range(self.num_parts):
            message_resp = self.start_consumer_backend()
            self.channel_backend.basic_publish(exchange='', routing_key='back_to_front_queue', body=message_resp)
            self.response = None

    def callback_actions_frontend(self, ch, method, properties, body):
        message_rec = body.decode("utf-8").split('_')
        self.filename = str(message_rec[0])
        self.hor_parts = int(message_rec[1])
        self.vert_parts = int(message_rec[2])
        self.hor_chunk /= self.hor_parts
        self.vert_chunk /= self.vert_parts
        self.num_parts = self.hor_parts * self.vert_parts
        self.response = True

    def callback_actions_backend(self, ch, method, properties, body):
        message_rec = body
        self.response = message_rec
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consumer_frontend(self):
        while self.response is None:
            self.connection_frontend.process_data_events()
        return self.response

    def start_consumer_backend(self):
        while self.response is None:
            self.connection_backend.process_data_events()
        return self.response


if __name__ == '__main__':
    workers = int(input("Number of backend workers: "))
    a = BackendCoord(num_workers=workers)
    a.get_minimaps()
