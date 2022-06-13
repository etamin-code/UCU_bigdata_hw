def write_product_reviews(data, session, product_table):
    product_reviews_df = data[["product_id", "star_rating", "review_body", "review_date"]]
    query = f"INSERT INTO {product_table}(product_id,star_rating,review_body, review_date) VALUES (?,?,?,?);"
    prepared = session.prepare(query)

    for _, item in product_reviews_df.iterrows():
        try:
            session.execute(prepared, (item.product_id, int(item.star_rating), item.review_body, item.review_date))
        except:
            print(f"skipping bad line in {product_table}")


def write_customer_reviews(data, session, customer_table):
    customer_reviews_df = data[["customer_id", "review_id", "product_id", "star_rating",
                                "review_body", "review_date", "verified_purchase"]]
    query = f"INSERT INTO {customer_table}(customer_id, review_id, product_id, star_rating," \
            f" review_body, review_date, verified_purchase) VALUES (?,?,?,?,?,?,?);"
    prepared = session.prepare(query)

    for _, item in customer_reviews_df.iterrows():
        try:
            session.execute(prepared, (item.customer_id, item.review_id, item.product_id,
                                       int(item.star_rating), item.review_body, item.review_date,
                                       item.verified_purchase))
        except:
            print(f"skipping bad line in {customer_table}")













