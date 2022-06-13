#!/bin/bash

declare -r KEY_SPACE='hw4_kukhar'

declare -r ddl="
CREATE KEYSPACE ${KEY_SPACE} WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE ${KEY_SPACE};

CREATE TABLE product_reviews(
    product_id text,
    star_rating int,
    review_body text,
    review_date date,
    PRIMARY KEY (product_id, star_rating, review_date));


CREATE TABLE customer_reviews(
    customer_id int,
    review_id text,
    product_id text,
    star_rating int,
    review_body text,
    review_date date,
    verified_purchase text,
    PRIMARY KEY (customer_id, star_rating, product_id, review_date));
"

echo "Started creating tables"
docker exec -it cassandra-node cqlsh -e "${ddl}"
echo "Finished creating tables"