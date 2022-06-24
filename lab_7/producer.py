import pandas as pd
import json
import datetime 
from time import sleep
from kafka import KafkaProducer


now = datetime.datetime.now
producer = KafkaProducer(bootstrap_servers='kafka-server:9092'
			  , value_serializer=lambda m: json.dumps(m).encode('ascii')
			  )


data = pd.read_csv('tweets.csv')

i = 0
while i < data.shape[0]:
    start = now()
    for _, row in data[i:i+15].iterrows():
        row['created_at'] = str(now())
        producer.send('tweets', dict(row))
    i += 15
    while (now() - start).seconds < 1:
        sleep(0.2)
producer.flush()

