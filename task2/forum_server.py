import sys
import pika
import time
import threading


class ForumServer:
    def __init__(self, logical_time=0):
        self.logical_time = logical_time
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange='send_from_server', exchange_type='fanout')

        result = self.channel.queue_declare(queue='', exclusive=False)
        self.queue_name = result.method.queue

    def update_time(self, new_time):
        if self.logical_time > new_time:
            self.logical_time += 1
        else:
            self.logical_time = new_time + 1

    def process_message(self, message):
        elements = message.split('_')
        new_logical_time = [-1]
        self.update_time(new_logical_time)
        print('User {} replied to message {} : {} (log_time : {})'.format(*elements))

    def send(self, text_message, response_to):
        self.update_time(-1)  # check if consuming thread updated a logical time
        message = '{}_{}_{}_{}'.format(self.nickname, text_message, response_to, self.logical_time)
        self.channel.basic_publish(exchange='logs', routing_key='', body=message)

    def callback_actions(self, ch, method, properties, body):
        debug_mode = 1
        with open('ny.osm') as osm_file:
            streets_all = soup.find_all('way')
            spisok = []
            i = 0
            for streets in streets_all:
                i += 1
                b = streets.find_all('tag')
                flag = 0
                for tag_line in b:
                    a = tag_line['k']
                    if 'highway' == a:
                        flag = 1
                    if 'name' == a and flag == 1 and tag_line['v'][0] == chr(letter):
                        spisok.append(tag_line['v'])

            spisok = pd.Series(spisok)
            spisok.drop_duplicates(inplace=True)
            if debug_mode == 0:
                message = str(len(spisok))
            else:
                for street in spisok:
                    print(street)
                message = str(len(spisok))

    def start_consumer(self):
        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback_actions(), auto_ack=True)
        self.channel.start_consuming()


if __name__ == '__main__':
    client = ForumServer()
    #client.send('Privet', 0)