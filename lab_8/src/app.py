from fastapi import FastAPI
import uvicorn
from cassandra.cluster import Cluster

from constants import *

app_host = "0.0.0.0"

app_port = 8080
app = FastAPI()


@app.get('/hello')
def test():
    print("hello")


@app.get('/fraud_transactions_for_user')
def transactions_for_user(nameOrig):
    cluster = Cluster([host], port=port)
    session = cluster.connect(keyspace)
    return list(session.execute(f"SELECT * FROM transactions WHERE nameOrig = '{nameOrig}' and isFraud = 1 ALLOW FILTERING;"))
    


@app.get('/the_biggest_transactions_of_user')
def the_biggest_transactions_of_user(nameOrig):
    cluster = Cluster([host], port=port)
    session = cluster.connect(keyspace)
    return list(session.execute(f"SELECT * FROM transactions WHERE nameOrig = '{nameOrig}' and isFraud = 1 LIMIT 3 ALLOW FILTERING;"))


@app.get('/total_input_for_user')
def total_input_for_user(nameOrig, start=None, end=None):
    cluster = Cluster([host], port=port)
    session = cluster.connect(keyspace)
    return list(session.execute(
        f"SELECT sum(amount) "
        f"FROM transactions "
        f"WHERE nameOrig = '{nameOrig}' "
        f"AND transaction_date >= '{start}' AND transaction_date <= '{end}' "
        f"ALLOW FILTERING;"))


if __name__ == "__main__":

    uvicorn.run(app, host=app_host, port=app_port)
    print("started app service")
        
