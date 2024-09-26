--Base table
WITH base_table AS (

SELECT DISTINCT user_id, gender, DATE(created_at) AS created_at, status, country, MIN(DATE_TRUNC(DATE(created_at), MONTH)) OVER (PARTITION BY user_id) AS first_purchase_month, COUNT(DISTINCT order_id) OVER (PARTITION BY user_id) no_of_orders
FROM(
SELECT orders.user_id, orders.order_id, orders.created_at, orders.num_of_item, order_items.product_id, products.retail_price, SUM(products.retail_price) OVER (PARTITION BY orders.order_id) AS order_value, orders.status, orders.gender, users.country
FROM bigquery-public-data.thelook_ecommerce.orders AS orders
INNER JOIN bigquery-public-data.thelook_ecommerce.order_items AS order_items
ON orders.order_id = order_items.order_id
INNER JOIN bigquery-public-data.thelook_ecommerce.products AS products
ON order_items.product_id = products.id
INNER JOIN bigquery-public-data.thelook_ecommerce.users AS users
ON orders.user_id = users.id
WHERE "2022-12-31 23:59:59 UTC" < orders.created_at AND orders.created_at < "2024-01-01 00:00:00 UTC" AND orders.status = 'Complete'
))

--Retention cohorts
SELECT 
gender,
country,
first_purchase_month, 
COUNT(DISTINCT user_id) AS n_customers_start,
COUNT(DISTINCT CASE WHEN created_at >= first_purchase_month AND created_at < DATE_ADD(first_purchase_month, INTERVAL 1 MONTH) THEN user_id ELSE NULL END) AS month_0,
COUNT(DISTINCT CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 1 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 2 MONTH) THEN user_id ELSE NULL END) AS month_1,
COUNT(DISTINCT CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 2 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 3 MONTH) THEN user_id ELSE NULL END) AS month_2,
COUNT(DISTINCT CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 3 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 4 MONTH) THEN user_id ELSE NULL END) AS month_3,
COUNT(DISTINCT CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 4 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 5 MONTH) THEN user_id ELSE NULL END) AS month_4,
COUNT(DISTINCT CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 5 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 6 MONTH) THEN user_id ELSE NULL END) AS month_5,
COUNT(DISTINCT CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 6 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 7 MONTH) THEN user_id ELSE NULL END) AS month_6,
COUNT(DISTINCT CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 7 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 8 MONTH) THEN user_id ELSE NULL END) AS month_7,
COUNT(DISTINCT CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 8 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 9 MONTH) THEN user_id ELSE NULL END) AS month_8,
COUNT(DISTINCT CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 9 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 10 MONTH) THEN user_id ELSE NULL END) AS month_9,
COUNT(DISTINCT CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 10 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 11 MONTH) THEN user_id ELSE NULL END) AS month_10,
COUNT(DISTINCT CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 11 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 12 MONTH) THEN user_id ELSE NULL END) AS month_11
FROM base_table
GROUP BY first_purchase_month, country, gender
ORDER BY country, first_purchase_month, gender
