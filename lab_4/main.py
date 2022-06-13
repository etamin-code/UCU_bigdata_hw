import pandas as pd
from cassandra.cluster import Cluster
from constants import *

from prepare_data import write_product_reviews, write_customer_reviews

cluster = Cluster([host], port=port)
session = cluster.connect(keyspace)

data = pd.read_csv(file_name, sep='\t', nrows=1000)

write_product_reviews(data, session, product_table)
write_customer_reviews(data, session, customer_table)

session.shutdown()
