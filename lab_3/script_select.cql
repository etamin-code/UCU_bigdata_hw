SELECT review_body
FROM product_reviews
WHERE product_id = 'ASD12034';

SELECT review_body
FROM product_reviews
WHERE product_id = 'ASD12034' AND star_rating = 4;

SELECT review_body
FROM customer_reviews
WHERE customer_id = 'ASD12034';

SELECT customer_id FROM customer_summary
WHERE num_of_reviews > N
AND reviewed_products CONTAINS 'ASD12034'

SELECT COUNT(review_id)
FROM customer_reviews
WHERE customer_id
AND star_rating >= 4;

