import pika
from bs4 import BeautifulSoup
import pandas as pd

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.queue_declare(queue='rpc_queue')  # establishing a queue


def on_request(ch, method, props, body):
    letter = chr(body)
    debug_mode = 0
    with open('ny.osm') as osm_file:
        soup = BeautifulSoup(osm_file, features="html.parser")
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
                if 'name' == a and flag == 1 and tag_line['v'][0] == letter:
                    spisok.append(tag_line['v'])

        spisok = pd.Series(spisok)
        spisok.drop_duplicates(inplace=True)
        if debug_mode == 0:
            message = str(len(spisok))
        else:
            c = ''
            for street in spisok:
                c = c + ' ' + str(street) + ','
            message = c

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id= \
                                                         props.correlation_id),
                     body=message)
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)

print(" [x] Awaiting RPC requests")
channel.start_consuming()
