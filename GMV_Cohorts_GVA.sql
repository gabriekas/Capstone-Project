--Base table
WITH base_table AS (

SELECT DISTINCT order_id, user_id, gender, DATE(created_at) AS created_at, order_value, order_cost, status, country, MIN(DATE_TRUNC(DATE(created_at), MONTH)) OVER (PARTITION BY user_id) AS first_purchase_month, COUNT(DISTINCT order_id) OVER (PARTITION BY user_id) no_of_orders
FROM(
SELECT orders.user_id, orders.order_id, orders.created_at, orders.num_of_item, order_items.product_id, products.retail_price, SUM(products.retail_price) OVER (PARTITION BY orders.order_id) AS order_value, orders.status, orders.gender, users.country, SUM(products.cost) OVER (PARTITION BY orders.order_id) AS order_cost
FROM bigquery-public-data.thelook_ecommerce.orders AS orders
INNER JOIN bigquery-public-data.thelook_ecommerce.order_items AS order_items
ON orders.order_id = order_items.order_id
INNER JOIN bigquery-public-data.thelook_ecommerce.products AS products
ON order_items.product_id = products.id
INNER JOIN bigquery-public-data.thelook_ecommerce.users AS users
ON orders.user_id = users.id
WHERE "2022-12-31 23:59:59 UTC" < orders.created_at AND orders.created_at < "2024-01-01 00:00:00 UTC" AND orders.status = 'Complete'
))

--GMV cohorts
SELECT 
gender,
country,
first_purchase_month, 
COUNT(DISTINCT user_id) AS n_customers_start,
SUM(no_of_orders) AS total_orders,
SUM(order_value) AS total_revenue,
SUM(order_cost) AS total_variable_costs,
SUM(CASE WHEN created_at >= first_purchase_month AND created_at < DATE_ADD(first_purchase_month, INTERVAL 1 MONTH) THEN order_value ELSE NULL END) AS month_0_tableau, --For tableau data connection
SUM(CASE WHEN created_at >= first_purchase_month AND created_at < DATE_ADD(first_purchase_month, INTERVAL 1 MONTH) THEN order_value ELSE NULL END) AS month_0,
SUM(CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 1 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 2 MONTH) THEN order_value ELSE NULL END) AS month_1,
SUM(CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 2 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 3 MONTH) THEN order_value ELSE NULL END) AS month_2,
SUM(CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 3 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 4 MONTH) THEN order_value ELSE NULL END) AS month_3,
SUM(CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 4 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 5 MONTH) THEN order_value ELSE NULL END) AS month_4,
SUM(CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 5 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 6 MONTH) THEN order_value ELSE NULL END) AS month_5,
SUM(CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 6 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 7 MONTH) THEN order_value ELSE NULL END) AS month_6,
SUM(CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 7 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 8 MONTH) THEN order_value ELSE NULL END) AS month_7,
SUM(CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 8 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 9 MONTH) THEN order_value ELSE NULL END) AS month_8,
SUM(CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 9 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 10 MONTH) THEN order_value ELSE NULL END) AS month_9,
SUM(CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 10 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 11 MONTH) THEN order_value ELSE NULL END) AS month_10,
SUM(CASE WHEN created_at >= DATE_ADD(first_purchase_month, INTERVAL 11 MONTH) AND created_at < DATE_ADD(first_purchase_month, INTERVAL 12 MONTH) THEN order_value ELSE NULL END) AS month_11
FROM base_table
GROUP BY first_purchase_month, country, gender
ORDER BY  first_purchase_month, gender




