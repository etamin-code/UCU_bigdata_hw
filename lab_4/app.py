from fastapi import FastAPI
import uvicorn
from cassandra.cluster import Cluster

from constants import *

app_host = "localhost"
app_port = 8080
app = FastAPI()


@app.get('/hello')
def test():
    print("hello")


@app.get('/reviews_for_product_id')
def reviews_for_product_id(product_id, star_rating=None):
    cluster = Cluster([host], port=port)
    session = cluster.connect(keyspace)
    if star_rating:
        reviews = list(session.execute(f"SELECT review_body FROM {product_table} WHERE product_id='{product_id}' AND star_rating={star_rating} ALLOW FILTERING;"))
    else:
        reviews = list(session.execute(f"SELECT review_body FROM {product_table} WHERE product_id='{product_id}' ALLOW FILTERING;"))
    return reviews


@app.get('/N_most_reviewed_items')
def N_most_reviewed_items(N=10, start=None, end=None):
    cluster = Cluster([host], port=port)
    session = cluster.connect(keyspace)
    given_period = ""
    if not (start and end):
        given_period = f"WHERE review_date >= '{start}' AND review_date <= '{end}'"
    return list(session.execute(
        f"SELECT product_id, COUNT(review_body) as num "
        f"FROM {product_table} "
        f"{given_period} "
        f"GROUP BY product_id "
        f"ORDER BY num DESC "
        f"LIMIT {N} "
        f"ALLOW FILTERING;"))

@app.get('/reviews_for_customer_id')
def reviews_for_customer_id(customer_id):
    cluster = Cluster([host], port=port)
    session = cluster.connect(keyspace)
    reviews = list(session.execute(f"SELECT review_body FROM {customer_table} WHERE customer_id={customer_id};"))
    return reviews

@app.get('/N_most_productive_customers')
def N_most_productive_customers(N=10, start=None, end=None):
    cluster = Cluster([host], port=port)
    session = cluster.connect(keyspace)
    given_period = ""
    if not (start and end):
        given_period = f" AND review_date >= '{start}' AND review_date <= '{end}' "
    return list(session.execute(
        f"SELECT customer_id, COUNT(review_body) as num "
        f"FROM {customer_table} "
        f" WHERE verified_purchase='Y' "
        f"{given_period} "
        f"GROUP BY customer_id "
        f"ORDER BY num DESC "
        f"LIMIT {N} "
        f"ALLOW FILTERING;"))

@app.get('/N_haters')
def N_haters(N, start=None, end=None):
    cluster = Cluster([host], port=port)
    session = cluster.connect(keyspace)
    list(session.execute(
        f"SELECT customer_id "
        f"FROM {customer_table} "
        f"WHERE review_date >= '{start}' AND review_date <= '{end}' "
        f"AND star_rating <= 2 "
        f"GROUP BY customer_id "
        f"ORDER BY COUNT(review_body) DESC "
        f"LIMIT {N} "
        f"ALLOW FILTERING;"))

@app.get('/N_backers')
def N_haters(N, start=None, end=None):
    cluster = Cluster([host], port=port)
    session = cluster.connect(keyspace)
    list(session.execute(
        f"SELECT customer_id "
        f"FROM {customer_table} "
        f"WHERE review_date >= '{start}' AND review_date <= '{end}' "
        f"AND star_rating >= 4 "
        f"GROUP BY customer_id "
        f"ORDER BY COUNT(review_body) DESC "
        f"LIMIT {N} "
        f"ALLOW FILTERING;"))


if __name__ == "__main__":

    print("started app service")
    uvicorn.run(app, host=app_host, port=app_port)