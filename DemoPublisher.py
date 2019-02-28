from time import sleep
from json import dumps

from confluent_kafka import Producer


def kafkaProducer():

    p = Producer({'bootstrap.servers': 'localhost:9092'})

    for e in range(100):

        k='key-'+str(e)
        p.produce('testkafka1', key='number', value=str(e))
        sleep(5)
    p.flush(30)

kafkaProducer()
