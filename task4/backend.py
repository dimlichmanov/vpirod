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

    def start_election(self):
        with self._lock:
            self.value = 1

    def finish_election(self):
        with self._lock:
            self.value = 0

    def check_value(self):
        with self._lock:
            return self.value


class Backend:
    def __init__(self, num, total_num):
        # Initialize connections
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.filename = None
        self.num = num
        self.UID = random.randint(0, 100)
        self.CR_queue = 'queue_{}'.format((num+1) % total_num)  # queue for Chang_roberts algorithm
        self.participate = Participate()

        result = self.channel.queue_declare(queue='worker_{}'.format(num), exclusive=True)
        self.queue_name = result.method.queue

        self.channel.confirm_delivery()

        self.CR_thread = threading.Thread(target=)  # Thread is listening to election messages
        # Sleeping on receive channel
        self.start_consumer()

    def callback_actions(self, ch, method, properties, body):
        # Split a message, retrieve coordinates and send a json file
        time.sleep(15)  # Accept that coordinator send all parts before workers started their run
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
                print('Message was published')
            except pika.exceptions.UnroutableError:
                print('Message was returned')
                # TODO run an election
            # self.channel.basic_publish(exchange='', routing_key='workers_to_coord_queue', body=res_intersect)

    def run_election(self):
        election_message = 'ELECTION_{}_{}'.format(self.num, self.UID)
        self.channel.basic_publish(exchange='', routing_key='workers_to_coord_queue', body=election_message)


    def cr_callback(self, ch, method, properties, body):
        parts = body.decode("utf-8").split('_')
        if parts[0] == 'ELECTION' and self.participate.check_value() == 0:
            self.participate.start_election()
            winner = max(int(parts[2]), self.UID)



    def start_consumer(self):
        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback_actions, auto_ack=True)
        self.channel.start_consuming()

    def acquire_queue(self):
        # If current process is elected
        pass



if __name__ == '__main__':
    worker_num = int(input('Enter a number of a worker: '))
    number_of_workers = int(input('Enter number of workers: '))
    server = Backend(num=worker_num, total=number_of_workers)

