import pandas as pd
import json
import datetime 
import random
from time import sleep
from kafka import KafkaProducer


now = datetime.datetime.now
producer = KafkaProducer(bootstrap_servers='kafka-server:9092'
			  , value_serializer=lambda m: json.dumps(m).encode('ascii')
			  )


data = pd.read_csv('data.csv')

month = datetime.timedelta(days=30)
rate = 25
i = 0
while i < data.shape[0]:
    start = now()
    for _, row in data[i:i+rate].iterrows():
        row['transaction_date'] = str(start - random.random() * month)
        producer.send('transactions_topic', dict(row))
    i += rate
    while (now() - start).seconds < 1:
        sleep(0.2)
producer.flush()

