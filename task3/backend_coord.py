import pika


class BackendCoord:
    def __init__(self, num_workers):
        self.filename = None
        self.hor_parts = None
        self.vert_parts = None
        self.num_parts = None
        self.num_workers = num_workers

        self.connection_frontend = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel_frontend = self.connection_frontend.channel()

        self.connection_backend = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel_backend = self.connection_backend.channel()

        self.properties = None
        self.response = None

        self.borders = [-125, -65, 21, 51]
        self.cents = [36, -95]
        self.ths = [31, 41, -105, -85]
        self.frs = [28.5, 36, 43.5, -110, -95, -80]

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
            bord = self.borders
            message = []
            if self.num_parts == 1:
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, bord[0], bord[2], bord[0], bord[3], bord[1], bord[3], bord[1], bord[2]))

            if self.num_parts == 2:
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.cents[1], bord[2], self.cents[1], bord[3], bord[1], bord[3], bord[1], bord[2]))

                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, bord[0], bord[2], bord[0], bord[3], self.cents[1], bord[3], self.cents[1], bord[2]))

            if self.num_parts == 4:
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, bord[0], bord[2], bord[0], self.cents[0], self.cents[1], self.cents[0], self.cents[1], bord[2]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, bord[0], self.cents[0], bord[0], bord[3], self.cents[1], bord[3], self.cents[1], self.cents[0]))

                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.cents[1], bord[2], self.cents[1], self.cents[0], bord[1], self.cents[0], bord[1], bord[2]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.cents[1], self.cents[0], self.cents[1], bord[3], bord[1], bord[3], bord[1], self.cents[0]))

            if self.num_parts == 9:
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, bord[0], bord[2],     bord[0], self.ths[0], self.ths[2], self.ths[0], self.ths[2], bord[2]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, bord[0], self.ths[0], bord[0], self.ths[1], self.ths[2], self.ths[1], self.ths[2], self.ths[0]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, bord[0], self.ths[1], bord[0], bord[3],     self.ths[2], bord[3],     self.ths[2], self.ths[1]))

                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.ths[2], bord[2],     self.ths[2], self.ths[0], self.ths[3], self.ths[0], self.ths[3], bord[2]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.ths[2], self.ths[0], self.ths[2], self.ths[1], self.ths[3], self.ths[1], self.ths[3], self.ths[0]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.ths[2], self.ths[1], self.ths[2], bord[3],     self.ths[3], bord[3],     self.ths[3], self.ths[1]))

                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.ths[3], bord[2],     self.ths[3], self.ths[0], bord[1], self.ths[0], bord[1], bord[2]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.ths[3], self.ths[0], self.ths[3], self.ths[1], bord[1], self.ths[1], bord[1], self.ths[0]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.ths[3], self.ths[1], self.ths[3], bord[3],     bord[1], bord[3],     bord[1], self.ths[1]))

            if self.num_parts == 16:
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, bord[0], bord[2],     bord[0], self.frs[0], self.frs[3], self.frs[0], self.frs[3], bord[2]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, bord[0], self.frs[0], bord[0], self.frs[1], self.frs[3], self.frs[1], self.frs[3], self.frs[0]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, bord[0], self.frs[1], bord[0], self.frs[2], self.frs[3], self.frs[2], self.frs[3], self.frs[1]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, bord[0], self.frs[2], bord[0], bord[3],     self.frs[3], bord[3],     self.frs[3], self.frs[2]))

                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.frs[3], bord[2],     self.frs[3], self.frs[0], self.frs[4], self.frs[0], self.frs[4], bord[2]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.frs[3], self.frs[0], self.frs[3], self.frs[1], self.frs[4], self.frs[1], self.frs[4], self.frs[0]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.frs[3], self.frs[1], self.frs[3], self.frs[2], self.frs[4], self.frs[2], self.frs[4], self.frs[1]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.frs[3], self.frs[2], self.frs[3], bord[3],     self.frs[4], bord[3],     self.frs[4], self.frs[2]))

                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.frs[4], bord[2],     self.frs[4], self.frs[0], self.frs[5], self.frs[0], self.frs[5], bord[2]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.frs[4], self.frs[0], self.frs[4], self.frs[1], self.frs[5], self.frs[1], self.frs[5], self.frs[0]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.frs[4], self.frs[1], self.frs[4], self.frs[2], self.frs[5], self.frs[2], self.frs[5], self.frs[1]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.frs[4], self.frs[2], self.frs[4], bord[3],     self.frs[5], bord[3],     self.frs[5], self.frs[2]))

                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.frs[5], bord[2],     self.frs[5], self.frs[0], bord[1], self.frs[0], bord[1], bord[2]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.frs[5], self.frs[0], self.frs[5], self.frs[1], bord[1], self.frs[1], bord[1], self.frs[0]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.frs[5], self.frs[1], self.frs[5], self.frs[2], bord[1], self.frs[2], bord[1], self.frs[1]))
                message.append('{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.filename, self.frs[5], self.frs[2], self.frs[5], bord[3],     bord[1], bord[3],     bord[1], self.frs[2]))

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
        self.num_parts = self.hor_parts * self.vert_parts
        self.response = True

    def callback_actions_backend(self, ch, method, properties, body):
        message_rec = body
        self.response = message_rec

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
