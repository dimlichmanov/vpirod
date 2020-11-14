import pika
import uuid


class Client:
    def __init__(self, filename, hor, vert):
        self.filename = filename
        self.hor_parts = hor
        self.vert_parts = vert
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
        message_send = '{}_{}_{}'.format(self.filename, self.hor_parts, self.vert_parts)  # New message type
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='', routing_key='frontend_queue', properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ), body=message_send)
        for i in range(self.hor_parts * self.vert_parts):
            resp = self.start_consumer()
            part_file = open("part_{}.geojson".format(i), "w")
            part_file.write(resp)
            part_file.close()
            self.response = None

    def callback_actions(self, ch, method, properties, body):
        self.response = body.decode("utf-8")

    def start_consumer(self):
        # Sleep on input queue
        while self.response is None:
            self.connection.process_data_events()
        return self.response


if __name__ == '__main__':
    horizontal_nums = int(input('Enter nums of horizontal blocks: '))
    vertical_nums = int(input('Enter nums of vertical blocks: '))

    geojson_full = './admin1-us.geojson'

    a = Client(geojson_full, hor=horizontal_nums, vert=vertical_nums)
    a.get_minimaps()
