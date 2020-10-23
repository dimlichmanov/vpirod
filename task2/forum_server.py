import pika


class ForumServer:
    def __init__(self, logical_time=0):
        self.logical_time = logical_time
        self.message_counter = 1
        init_message = 'Welcome to chat (time={})'.format(self.logical_time)
        self.messages = [init_message]
        self.hierarchy = [[]]
        self.current_history=""

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

        # Sleeping on receive channel
        self.start_consumer()

    def update_time(self, new_time):
        if self.logical_time > new_time:
            self.logical_time += 1
        else:
            self.logical_time = new_time + 1

    def gain_history(self, num_thread, num_tabs):
        for i in self.hierarchy[num_thread]:
            self.current_history += (str('\t')*num_tabs + str(self.messages[i]) + str('\n'))
            if len(self.hierarchy[i]) > 0:
                self.gain_history(i, num_tabs+1)

    def callback_actions(self, ch, method, properties, body):
        # Split a message, retrieve a logical time, send a client-friendly message to clients
        elements = body.decode("utf-8").split('_')
        new_logical_time = int(elements[-1])
        self.update_time(new_logical_time)
        if len(elements) > 2:
            response_to = int((elements[-2]))
            self.hierarchy.append([])
            self.hierarchy[response_to].append(self.message_counter)
            self.messages.append('#{} - {} answers on {}: '.format(self.message_counter, elements[0], response_to) + str(elements[-4]) + '(time:{})'.format(self.logical_time))
            self.message_counter += 1
            print(self.hierarchy)
        else:
            self.current_history = self.messages[0]+'\n'
            self.gain_history(0, 1)
            print(self.current_history)
            self.channel_send.basic_publish(exchange='', routing_key=str(elements[0]), body=self.current_history)

    def start_consumer(self):
        self.channel_receive.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback_actions, auto_ack=True)
        self.channel_receive.start_consuming()


if __name__ == '__main__':
    server = ForumServer()
