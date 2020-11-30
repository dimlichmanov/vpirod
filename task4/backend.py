import geopandas
import numpy as np
import rtree
import pygeos
import json
from shapely.geometry import Polygon
import pika
import random
import pika.exceptions as exceptions
import time
import threading


class Participate(object):
    def __init__(self):
        self.value = 0
        self._lock = threading.Lock()
        self.leader = 0

    def start_election(self):
        with self._lock:
            self.value = 1

    def finish_election(self, if_leader):
        with self._lock:
            self.value = 0
            self.leader = if_leader

    def check_value(self):
        with self._lock:
            return self.value

    def check_leader(self):
        with self._lock:
            return self.leader


class Backend:
    def __init__(self, num, total_num):
        # Initialize connections
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.filename = None
        self.num = num
        self.total_num = total_num
        # self.UID = random.randint(0, 100)
        self.UID = 100 - num
        self.participate = Participate()
        self.round_robin = 0
        self.connection_frontend = None
        self.connection_backend = None
        self.properties = None
        self.response = None
        self.channel_frontend = None
        self.channel_backend = None
        self.query_queue = None
        self.callback_queue = None
        self.transfer = False
        self.new_leader = False

        result = self.channel.queue_declare(queue='worker_{}'.format(num), exclusive=True)
        self.queue_name = result.method.queue

        self.channel.confirm_delivery()

        self.CR_thread = threading.Thread(target=self.CR_function)
        self.CR_thread.start()

        # Sleeping on receive channel
        self.start_consumer()

    def callback_actions(self, ch, method, properties, body):
        # Split a message, retrieve coordinates and send a json file
        if self.num == 0:
            time.sleep(15)  # Accept that coordinator send all parts before workers started their run
        else:
            time.sleep(25)
        els = body.decode("utf-8").split('_')
        if len(els) > 1:  # Working with coordinates
            self.filename = str(els[0])
            file = geopandas.read_file(self.filename)
            file.drop(index=[3, 50], inplace=True)
            quadrant = geopandas.GeoSeries([Polygon([(float(els[1]), float(els[2])), (float(els[3]), float(els[4])), (float(els[5]), float(els[6])), (float(els[7]), float(els[8]))])])
            df1 = geopandas.GeoDataFrame({'geometry': quadrant, 'df1': [1]})
            res_intersect = geopandas.overlay(df1, file, how='intersection')
            res_intersect = res_intersect.to_json()
            try:
                self.channel.basic_publish(exchange='',
                                           routing_key='workers_to_coord_queue',
                                           body=res_intersect,
                                           properties=pika.BasicProperties(content_type='text/plain',
                                                                           delivery_mode=1),
                                           mandatory=True)
                print('Sent')
            except pika.exceptions.UnroutableError:
                print('Error')
                if self.participate.check_value() == 0:  # Если мы первые кто обнаружил проблему
                    self.run_election()
                self.CR_thread.join()
                print('Joined thread')
                if self.participate.check_leader():
                    print('Joined thread LEADER')
                    self.new_leader = True
                else:
                    print('not leader')
                self.channel.basic_publish(exchange='',
                                           routing_key='back_to_front_queue',
                                           body=res_intersect)

    # Methods for second thread

    def CR_function(self):
        self.counter = 0
        self.cr_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.cr_channel = self.cr_connection.channel()

        result = self.cr_channel.queue_declare(queue='cr_{}'.format(self.num))
        self.cr_queue_name = result.method.queue

        self.cr_channel.basic_consume(
            queue=self.cr_queue_name,
            on_message_callback=self.cr_callback,
            auto_ack=True)

        while self.counter != 2:
            time.sleep(1)
            self.cr_connection.process_data_events()
        return

    def cr_callback(self, ch, method, properties, body):
        print('RECEIVED')
        parts = body.decode("utf-8").split('_')
        if parts[0] == 'ELECTION' and self.participate.check_value() == 0: # где то приняли сообщение, мы не инициаторы
            self.counter = 1
            self.participate.start_election()
            winner = max(int(parts[2]), self.UID)
            election_message = 'ELECTION_{}_{}'.format(int(parts[1]), winner)
            print('tut1', election_message)
            right_neighbor = (self.num + 1) % self.total_num
            self.cr_channel.basic_publish(exchange='', routing_key='cr_{}'.format(right_neighbor), body=election_message)

        elif parts[0] == 'ELECTION' and self.participate.check_value() == 1 and int(parts[1]) == self.num:  # к инициатору вернулось сообщение
            self.counter = 2
            print('SEND VOTAGE MESSAGE')
            election_message = 'ELECTED_{}_{}'.format(int(parts[1]), int(parts[2]))
            right_neighbor = (self.num + 1) % self.total_num
            print('tut2', election_message)
            self.participate.finish_election(int(parts[2]) == self.UID)
            self.channel.basic_publish(exchange='', routing_key='cr_{}'.format(right_neighbor), body=election_message)

        elif parts[0] == 'ELECTION' and self.participate.check_value() == 1 and int(parts[1]) != self.num:
            print('tut3')
            pass

        elif parts[0] == 'ELECTED' and self.participate.check_value() == 1: # выходим из голосования
            self.counter = 2
            election_message = 'ELECTED_{}_{}'.format(int(parts[1]), int(parts[2]))
            right_neighbor = (self.num + 1) % self.total_num
            print('tut4', election_message)
            self.participate.finish_election(int(parts[2]) == self.UID)
            if right_neighbor != 0:
                self.channel.basic_publish(exchange='', routing_key='cr_{}'.format(right_neighbor), body=election_message)

    # Methods for election

    def run_election(self):
        election_message = 'ELECTION_{}_{}'.format(self.num, self.UID)
        right_neighbor = (self.num+1) % self.total_num
        self.participate.start_election()
        print(election_message)
        self.channel.basic_publish(exchange='', routing_key='cr_{}'.format(right_neighbor), body=election_message)

    # Methods for consumer leader

    def start_consumer_leader(self):
        self.channel.basic_publish(exchange='', routing_key='worker_{}'.format(self.num), body='ELECTED_0')
        while self.transfer is False:
            self.connection.process_data_events()

    def callback_actions_leader(self, ch, method, properties, body):
        els = body.decode("utf-8").split('_')
        if els[0] == "ELECTED":
            self.transfer = True
        else:
            if self.round_robin != self.num:
                self.channel.basic_publish(exchange='', routing_key='worker_{}'.format(self.round_robin), body=body)
                self.round_robin = (self.round_robin + 1) % self.total_num
                print(self.round_robin)
            else:
                self.round_robin = (self.round_robin + 1) % self.total_num
                self.channel.basic_publish(exchange='', routing_key='worker_{}'.format(self.round_robin), body=body)
                print(self.round_robin)

    # Consumer leader

    def callback_actions_backend(self, ch, method, properties, body):
        message_rec = body
        self.response = message_rec

    def start_consumer_backend(self):
        while self.response is None:
            self.connection_backend.process_data_events()
        return self.response

    def start_consumer(self):
        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback_actions, auto_ack=True)
        while self.new_leader is False:
            self.connection.process_data_events()
        if self.new_leader:
            self.start_consumer_leader()  # Раскидали сообщения
            self.connection_frontend = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            self.channel_frontend = self.connection_frontend.channel()

            self.connection_backend = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            self.channel_backend = self.connection_backend.channel()

            self.properties = None
            self.response = None

            result = self.channel_frontend.queue_declare(queue='backend_coord_queue', exclusive=False)  # Queue for queries from frontend
            self.query_queue = result.method.queue
            result = self.channel_backend.queue_declare(queue='workers_to_coord_queue', exclusive=False)  # Queue for backend-frontend communication
            self.callback_queue = result.method.queue

            self.channel_backend.basic_consume(
                queue=self.callback_queue,
                on_message_callback=self.callback_actions_backend,
                auto_ack=True)

            while True:
                message_resp = self.start_consumer_backend()
                self.channel_backend.basic_publish(exchange='', routing_key='back_to_front_queue', body=message_resp)
                self.response = None


if __name__ == '__main__':
    worker_num = int(input('Enter a number of a worker: '))
    number_of_workers = int(input('Enter number of workers: '))
    server = Backend(num=worker_num, total_num=number_of_workers)

