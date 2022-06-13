CREATE  KEYSPACE reviews WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE reviews;


CREATE TABLE product_reviews(
    product_id text,   
    star_rating int,
    review_body text,
    PRIMARY KEY (product_id, star_rating));
    

CREATE TABLE customer_reviews(
    customer_id text,
    review_id text,
    product_id text,   
    star_rating int,
    review_body text,
    PRIMARY KEY (customer_id, star_rating, product_id));

CREATE TABLE customer_summary(
    customer_id int,
    num_of_reviews int,
    reviewed_products list<text>,
    PRIMARY KEY(customer_id, total_reviews_amount))
