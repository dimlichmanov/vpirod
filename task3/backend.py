import geopandas
import numpy as np
import rtree
import pygeos
import json
from shapely.geometry import Polygon
import pika


class Backend:
    def __init__(self, num):
        # Initialize connections
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.filename = None

        result = self.channel.queue_declare(queue='worker_{}'.format(num), exclusive=True)
        self.queue_name = result.method.queue

        # Sleeping on receive channel
        self.start_consumer()

    def callback_actions(self, ch, method, properties, body):
        # Split a message, retrieve coordinates and send a json file
        els = body.decode("utf-8").split('_')
        print('ok')
        if len(els) > 1:  # Working with coordinates
            self.filename = str(els[0])
            file = geopandas.read_file(self.filename)
            file.drop(index=[3, 50], inplace=True)
            quadrant = geopandas.GeoSeries([Polygon([(float(els[1]), float(els[2])), (float(els[3]), float(els[4])), (float(els[5]), float(els[6])), (float(els[7]), float(els[8]))])])
            df1 = geopandas.GeoDataFrame({'geometry': quadrant, 'df1': [1]})
            res_intersect = geopandas.overlay(df1, file, how='intersection')
            res_intersect = res_intersect.to_json()
            self.channel.basic_publish(exchange='', routing_key='workers_to_coord_queue', body=res_intersect)
            print('send')

    def start_consumer(self):
        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback_actions, auto_ack=True)
        self.channel.start_consuming()


if __name__ == '__main__':
    worker_num = int(input('Enter a number of a worker: '))
    server = Backend(num=worker_num)
