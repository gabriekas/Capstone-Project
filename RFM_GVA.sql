--Base table
WITH base_table AS (
SELECT DISTINCT order_id, user_id, gender, created_at, order_value, order_cost, status, country
FROM(
SELECT orders.user_id, orders.order_id, orders.created_at, orders.num_of_item, order_items.product_id, products.retail_price, SUM(products.retail_price) OVER (PARTITION BY orders.order_id) AS order_value, orders.status, orders.gender, users.country, SUM(products.cost) OVER (PARTITION BY orders.order_id) AS order_cost
FROM bigquery-public-data.thelook_ecommerce.orders AS orders
INNER JOIN bigquery-public-data.thelook_ecommerce.order_items AS order_items
ON orders.order_id = order_items.order_id
INNER JOIN bigquery-public-data.thelook_ecommerce.products AS products
ON order_items.product_id = products.id
INNER JOIN bigquery-public-data.thelook_ecommerce.users AS users
ON orders.user_id = users.id
WHERE "2022-12-31 23:59:59 UTC" < orders.created_at AND orders.created_at < "2024-01-01 00:00:00 UTC" AND orders.status IN ('Complete', 'Returned', 'Cancelled') --Exclude orders that were not completed as including it would mislead actual customer engagement and include returned and canceled orders to complement RFM calculation
)),

--Computation for Frequency & Monetary & Return & Cancelations
F_M_Ret_C AS (
SELECT 
user_id,
gender,
country,
MAX(CASE WHEN status = 'Complete' THEN (TIMESTAMP(created_at)) END) AS last_order_date,
NULLIF(COUNT(DISTINCT CASE WHEN status = 'Complete' THEN order_id END), 0) AS frequency,
SUM(CASE WHEN status = 'Complete' THEN order_value END) AS monetary,
SUM(CASE WHEN status = 'Complete' THEN order_cost END) AS variable_costs,
COUNT(DISTINCT CASE WHEN status = 'Returned' THEN order_id END) AS returns,
COUNT(DISTINCT CASE WHEN status = 'Cancelled' THEN order_id END) AS cancelations,
FROM base_table 
GROUP BY user_id, gender, country
),

--Computation for Recenecy
R AS (
SELECT *,
DATE_DIFF(ref_date, last_order_date, DAY) AS recency
FROM(
  SELECT *,
  MAX(last_order_date) OVER () AS ref_date
  FROM F_M_Ret_C)
),

--Determination of quartiles for RFM & Returns & Cancelations metrics
Quartiles AS (
SELECT
R.*,
--Percentiles for MONETARY
quantiles_m.percentiles[offset(25)] AS m25,
quantiles_m.percentiles[offset(50)] AS m50,
quantiles_m.percentiles[offset(75)] AS m75,
--Percentiles for FREQUENCY
quantiles_f.percentiles[offset(25)] AS f25,
quantiles_f.percentiles[offset(50)] AS f50,
quantiles_f.percentiles[offset(75)] AS f75,
--Percentiles for RECENCY
quantiles_rec.percentiles[offset(25)] AS rec25,
quantiles_rec.percentiles[offset(50)] AS rec50,
quantiles_rec.percentiles[offset(75)] AS rec75,
--Percentiles for RETURNS
quantiles_ret.percentiles[offset(25)] AS ret25,
quantiles_ret.percentiles[offset(50)] AS ret50,
quantiles_ret.percentiles[offset(75)] AS ret75,
--Percentiles for CANCELLATIONS
quantiles_c.percentiles[offset(25)] AS c25,
quantiles_c.percentiles[offset(50)] AS c50,
quantiles_c.percentiles[offset(75)] AS c75
FROM 
R,
(SELECT APPROX_QUANTILES(monetary, 100) AS percentiles FROM R) quantiles_m,
(SELECT APPROX_QUANTILES(frequency, 100) AS percentiles FROM R) quantiles_f,
(SELECT APPROX_QUANTILES(recency, 100) AS percentiles FROM R) quantiles_rec,
(SELECT APPROX_QUANTILES(returns, 100) AS percentiles FROM R) quantiles_ret,
(SELECT APPROX_QUANTILES(cancelations, 100) AS percentiles FROM R) quantiles_c
),

--Scores for RFM, Returns and Cancellations metrics
RFM_Ret_C_scores AS (
SELECT *,
CASE WHEN rec_score != 0 AND f_score != 0 AND m_score != 0 THEN CAST(CONCAT(rec_score, f_score, m_score) AS INT) ELSE 0 END AS rfm_score
FROM(
SELECT *,
(CASE WHEN monetary IS NULL THEN 0
      WHEN monetary <= m25 THEN 1
      WHEN monetary <= m50 AND monetary > m25 THEN 2
      WHEN monetary <=m75 AND monetary > m50 THEN 3
      WHEN monetary > m75 THEN 4
END) AS m_score,
(CASE WHEN frequency IS NULL THEN 0
      WHEN frequency <= f25 THEN 1
      WHEN frequency <= f50 AND frequency > f25 THEN 2
      WHEN frequency <=f75 AND frequency > f50 THEN 3
      WHEN frequency > f75 THEN 4
END) AS f_score,
--Recency scoring is reversed as more recent customers should be scored higher
(CASE WHEN recency IS NULL THEN 0
      WHEN recency <= rec25 THEN 4
      WHEN recency <= rec50 AND recency > rec25 THEN 3
      WHEN recency <=rec75 AND recency > rec50 THEN 2
      WHEN recency > rec75 THEN 1
END) AS rec_score,
(CASE WHEN returns <= ret25 THEN 1
      WHEN returns <= ret50 AND returns > ret25 THEN 2
      WHEN returns <=ret75 AND returns > ret50 THEN 3
      WHEN returns > ret75 THEN 4
END) AS ret_score,
(CASE WHEN cancelations <= c25 THEN 1
      WHEN cancelations <= c50 AND cancelations > c25 THEN 2
      WHEN cancelations <=c75 AND cancelations > c50 THEN 3
      WHEN cancelations > c75 THEN 4
END) AS c_score,
FROM Quartiles
))

--Customer Segmentations
SELECT
user_id,
gender,
country,
recency,
frequency, 
monetary,
variable_costs,
returns,
cancelations,
rfm_score,
ret_score,
c_score,
CASE
WHEN rfm_score IN (334, 343, 344, 433, 434, 443, 444) AND ret_score <= 4 AND c_score <= 4 THEN 'Best Customers'
WHEN rfm_score IN (224, 233, 234, 243, 244, 324, 333) AND ret_score <= 4 AND c_score <= 4 THEN 'Big Spenders'
WHEN rfm_score IN (212, 222, 242, 311, 312, 321, 322, 331, 342, 341, 421, 422, 431, 432, 441, 442) AND ret_score <= 4 AND c_score <= 4 THEN 'Loyal Customers'
WHEN rfm_score IN (313, 314, 411, 412, 413, 414) AND ret_score <= 4 AND c_score <= 4 THEN 'Promising Customers'
WHEN rfm_score IN (132, 213, 214, 223, 232, 323, 332, 423, 424) AND ret_score <= 4 AND c_score <= 4 THEN 'Need Attention'
WHEN rfm_score IN (122, 142, 143, 144, 211, 221, 231, 241) AND ret_score <= 4 AND c_score <= 4 THEN 'Customers at Risk'
WHEN rfm_score IN (113, 114, 123, 124, 133, 134) AND ret_score <= 4 AND c_score <= 4 THEN 'Cannot be Lost'
WHEN rfm_score IN (111, 112, 121, 131, 141) AND ret_score <= 4 AND c_score <= 4 THEN 'Lost Customers'
WHEN rfm_score = 0 AND 3 <= ret_score  AND 1 <= c_score THEN 'Frequent Returners'
WHEN rfm_score = 0 AND 1 <= ret_score AND 3 <= c_score THEN 'Order Bailers' 
END AS customer_segment
FROM RFM_Ret_C_scores
