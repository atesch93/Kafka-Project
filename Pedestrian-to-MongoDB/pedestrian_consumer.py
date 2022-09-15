import json
from kafka import KafkaProducer

from kafka import KafkaConsumer
# from config.kafka_config import *
import json


class PdstrianConsumer:
	def __init__(self):
		group_id = "pedestrian_API"

		self.consumer = KafkaConsumer(
			client_id="client1",
			group_id=group_id,
			bootstrap_servers='kafka-ffee791-karampanah927-22f4.aivencloud.com:26471',
			security_protocol="SSL",
			ssl_cafile="ca.pem",
			ssl_certfile="service.cert",
			ssl_keyfile="service.key",
			value_deserializer=lambda v: json.loads(v.decode('ascii')),
			key_deserializer=lambda v: json.loads(v.decode('ascii')),
			max_poll_records=10
		)
		self.consumer.subscribe("pedestrian_API")



	def recieve(self):
		for message in self.consumer:
			print("recieved", message.key,message.value)
		print("finished")

