import json
from kafka import KafkaProducer

from kafka import KafkaConsumer
# from config.kafka_config import *
import json
from pdstrian_API import Hystreet_API

class PdstrianProducer:
	def __init__(self,city,API_key):
		self.producer = KafkaProducer(
			bootstrap_servers='kafka-ffee791-karampanah927-22f4.aivencloud.com:26471',
			security_protocol="SSL",
			ssl_cafile="ca.pem",
			ssl_certfile="service.cert",
			ssl_keyfile="service.key",
			value_serializer=lambda v: json.dumps(v).encode('utf-8'),
			key_serializer=lambda v: json.dumps(v).encode('utf-8') )

		self.hystreet = Hystreet_API(city,API_key)


	def send(self,date):
		data = self.hystreet.get_pdstrian_info()[0:3]
		ts = self.hystreet.get_pdstrian_info()[3]
		self.producer.send(topic="pedestrian_API ", key=str(ts), value=data)
		print(f"pedestrian for date {ts} was sent")
		self.producer.flush()
