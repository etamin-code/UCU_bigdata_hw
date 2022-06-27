import pandas as pd
import json
import datetime 
from time import sleep
from kafka import KafkaConsumer
import os
from cassandra.cluster import Cluster
from constants import *



cluster = Cluster([host], port=port)
session = cluster.connect(keyspace)

query = f"INSERT INTO transactions(step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,\
       			      nameDest, oldbalanceDest, newbalanceDest, isFraud, \
       			      isFlaggedFraud, transaction_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?);"
       			      
columns = ['step', 'type', 'amount', 'nameOrig', 'oldbalanceOrg',
	   'newbalanceOrig', 'nameDest', 'oldbalanceDest', 'newbalanceDest',
	   'isFraud', 'isFlaggedFraud', 'transaction_date']
prepared = session.prepare(query)


       


now = datetime.datetime.now
consumer = KafkaConsumer('transactions_topic'
			  , bootstrap_servers='kafka-server:9092'
			  , value_deserializer=lambda m: json.loads(m.decode('ascii'))
			  )

	
num = 0
for message in consumer:
    row = message.value
    row['transaction_date'] = datetime.datetime.strptime(row['transaction_date'].split('.')[0], '%Y-%m-%d %H:%M:%S')

    try:
        session.execute(prepared, [row[col] for col in columns])
        num += 1
      
    except:
        print(f"skipping bad line in transactions")
        
print(f"{num} messages is saved") 
session.shutdown()

   
