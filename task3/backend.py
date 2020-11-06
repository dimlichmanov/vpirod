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
        # Split a message, retrieve a logical time, send a client-friendly message to clients
        els = body.decode("utf-8").split('_')
        if len(els) > 1:  # Working with coordinates
            file = geopandas.read_file(self.filename)
            file.drop(index=[3, 50], inplace=True)
            quadrant = geopandas.GeoSeries([Polygon([(els[0], els[1]), (els[2], els[3]), (els[4], els[5]), (els[6], els[7])])])
            df1 = geopandas.GeoDataFrame({'geometry': quadrant, 'df1': [1]})
            res_intersect = geopandas.overlay(df1, file, how='intersection')
            res_intersect = json.dumps(res_intersect.to_json())
            self.channel.basic_publish(exchange='', routing_key='workers_to_coord', body=res_intersect)
        else:  # Working with file parameter
            self.filename = els[0]

    def start_consumer(self):
        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback_actions, auto_ack=True)
        self.channel.start_consuming()


if __name__ == '__main__':
    worker_num = int(input('Enter a number of a worker: '))
    server = Backend(num=worker_num)
