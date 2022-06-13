import numpy as np


def aggregate_for_product_reviews(df):
    cur_df = df[["product_id", "star_rating", "review_body"]]
    return cur_df