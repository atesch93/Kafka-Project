import json
import os
import time

from dotenv import load_dotenv
from kafka.consumer import KafkaConsumer
from kafka import TopicPartition

from pymongo import MongoClient

cluster=MongoClient("mongodb+srv://Sasha:9eeJEgyGYkZPc6VY@cluster0.i2q1kde.mongodb.net/?retryWrites=true&w=majority")
db=cluster["API_data"]
collection=db["weather"]

load_dotenv(verbose=True)

def main():
    consumer = KafkaConsumer(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
                          group_id=os.environ['CONSUMER_GROUP'],
                          value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                          enable_auto_commit=False,
                          auto_offset_reset='earliest',
                          security_protocol="SSL",
                          ssl_cafile="ca.pem",
                        ssl_certfile="service.cert",
                           ssl_keyfile="service.key")

    consumer.subscribe([os.environ['TOPICS_NAME']])

#tp = TopicPartition(os.environ['TOPICS_NAME'], 0)
#consumer.assign([tp])

#consumer.seek_to_end(tp)
#lastOffset = consumer.position(tp)
#consumer.seek_to_beginning(tp)


    count=0
    for record in consumer:
        print(record.value)
        value = {}
        count+=1
        try:
            value["_id"]=record.value["current"].pop("dt")
            value.update(record.value["current"])
            print(value)
            print(count)
            collection.insert_one(value)
        except:
            continue

if __name__ == '__main__':
    main()
