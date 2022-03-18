from kafka import KafkaProducer
import json
import sys
import time


def prod(topic: str):
    prod = KafkaProducer(bootstrap_servers=['localhost:9092'], )
                         # value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    while True:
        with open('record.json', 'rb') as f:

            prod.send(topic, f.read())
            time.sleep(3)

    prod.close()
