import pandas as pd
import json
import datetime 
from time import sleep
from kafka import KafkaConsumer
import os



now = datetime.datetime.now
consumer = KafkaConsumer('tweets'
			  , bootstrap_servers='kafka-server:9092'
			  , value_deserializer=lambda m: json.loads(m.decode('ascii'))
			  )

	
cur_data = pd.DataFrame(columns=['author_id', 'created_at', 'text'])
cur_time = None

for message in consumer:
    print(message)
    created_at = message.value['created_at']
    if cur_time != created_at[:16]:
        if not cur_time:
            print(1)
            cur_time = created_at[:16]
        else:
            print(2)
            cur_time = created_at[:16]
            cur_data.to_csv(f'./tweets_{created_at[8:10]}_{created_at[5:7]}_{created_at[0:4]}_{created_at[11:13]}_{created_at[14:16]}.csv', mode='a', index=False)
            print(f"wrote to tweets_{created_at[8:10]}_{created_at[5:7]}_{created_at[0:4]}_{created_at[11:13]}_{created_at[14:16]}.csv")
            cur_data = pd.DataFrame(columns=['author_id', 'created_at', 'text'])
            
    cur_data = pd.concat([cur_data, pd.DataFrame({'author_id': message.value['author_id'], 'created_at': message.value['created_at'], 'text': message.value['text']}, index=[0])], ignore_index = True, axis = 0)
     
   
