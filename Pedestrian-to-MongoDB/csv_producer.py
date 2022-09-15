import csv
import json
import time
from _csv import reader
from datetime import datetime
from itertools import chain
from kafka import KafkaProducer
import pandas as pd

class PedestrainCsvAPI:
	def __init__(self):
		self.mypath="D:\\UNI\\data_engineering2\\heidelberg-hauptstrasse11.csv"
		#"D:\\UNI\\data_engineering2\\heidelberg.csv"
		self.step = 1
	def csv_reader(self):
		with open(self.mypath,'r') as myfile:
			read_file = reader(myfile)
			mylist=[]
			for idx,row in enumerate(read_file):
				if idx ==0:
					continue
				else:
					mylist.append(row)
					if (idx+1)%self.step == 0:
						yield mylist
						mylist.clear()


	# def api_simulator(self):
	# 	for row in self.csv_reader():
	# 		time.sleep(0.2)
	# 		row2 = ''.join(chain(*row))
	# 		return(row2)
class Pdstrian_producer:
	def __init__(self):
		self.producer = KafkaProducer(
			bootstrap_servers='kafka-298b6db9-karampanah2491-ed66.aivencloud.com:26636',
			security_protocol="SSL",
			ssl_cafile="ca.pem",
			ssl_certfile="service.cert",
			ssl_keyfile="service.key",
			value_serializer=lambda v: json.dumps(v).encode('utf-8'),
			key_serializer=lambda v: json.dumps(v).encode('utf-8'))

	def send_csv_msg(self):
		pedestrain_instance = PedestrainCsvAPI()
		for row in pedestrain_instance.csv_reader():
			row = [i.strip() for i in row[0]]
			time.sleep(0.2)
			date = pd.to_datetime(" ".join(row[2].split(" ")[:2]), format='%Y-%m-%d %H:%M:%S')
			base = datetime(1970, 1, 1)
			ts = int((date - base).total_seconds())
			print(f"row : {row},\n date :{ts}")
			self.producer.send(topic="pedestrian_csv", key=str(ts), value=" ".join(row))
			print(f"weather for date {date} was sent")
			self.producer.flush()



pd_producer=Pdstrian_producer()
print(f"message was sent:{pd_producer.send_csv_msg()}")


