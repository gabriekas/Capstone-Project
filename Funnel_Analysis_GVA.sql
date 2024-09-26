--Only include sessions that started and ended in 2023
WITH sessions_base AS (
  SELECT *
  FROM ( 
  SELECT user_id,
  session_id,
  MIN(created_at) AS session_start,
  MAX(created_at) AS session_end
  FROM bigquery-public-data.thelook_ecommerce.events
  GROUP BY user_id, session_id)
  WHERE TIMESTAMP("2022-12-31 23:59:59 UTC") < session_start 
  AND session_start < TIMESTAMP("2024-01-01 00:00:00 UTC") 
  AND session_end < TIMESTAMP("2024-01-01 00:00:00 UTC")
  ),

--Filter unique events for users with ID (to remove duplicate events within the same session)
  earliest_event_with_id AS (
  SELECT user_id,
  session_id,
  event_type,
  MIN(created_at) as event_occurence
  FROM bigquery-public-data.thelook_ecommerce.events
  WHERE user_id IS NOT NULL
  GROUP BY user_id, session_id, event_type
  ),

-- Filter unique events for users without ID (to remove duplicate events within the same session)
  earliest_event_without_id AS (
  SELECT session_id,
  event_type,
  MIN(created_at) as event_occurence
  FROM bigquery-public-data.thelook_ecommerce.events
  WHERE user_id IS NULL
  GROUP BY session_id, event_type
  ),

--Combine events
  combined AS (
  SELECT *,
  DATE_TRUNC(created_at, MONTH) AS event_month,
  (CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END) AS registered_user_flag,
  (CASE WHEN COUNT(DISTINCT session_id) OVER (PARTITION BY user_id) > 1 AND user_id IS NOT NULL THEN 1 ELSE 0 END) AS returning_user_flag
  FROM (SELECT DISTINCT events.*, users.country
        FROM bigquery-public-data.thelook_ecommerce.events events
        INNER JOIN earliest_event_with_id events_id
        ON events.user_id = events_id.user_id
        AND events.created_at = events_id.event_occurence
        AND events.event_type = events_id.event_type
        INNER JOIN sessions_base
        ON events.session_id = sessions_base.session_id
        LEFT JOIN bigquery-public-data.thelook_ecommerce.users users
        ON events.user_id = users.id AND events.state = users.state 

        UNION ALL

        SELECT DISTINCT events.*, users.country
        FROM bigquery-public-data.thelook_ecommerce.events events
        INNER JOIN earliest_event_without_id event_no_id
        ON events.session_id = event_no_id.session_id
        AND events.created_at = event_no_id.event_occurence
        AND events.event_type = event_no_id.event_type
        INNER JOIN sessions_base
        ON event_no_id.session_id = sessions_base.session_id
        LEFT JOIN bigquery-public-data.thelook_ecommerce.users users
        ON events.state = users.state)
        )

  --Main query
SELECT
  (CASE 
  WHEN event_type = 'home' THEN 1
  WHEN event_type = 'department' THEN 2
  WHEN event_type = 'product' THEN 3
  WHEN event_type = 'cart' THEN 4
  WHEN event_type = 'purchase' THEN 5 
  ELSE 0 END
  ) AS event_order, 
  CASE WHEN country IS NULL THEN 'Country Unknown'
  WHEN country = 'España' THEN 'Spain' ELSE country END AS country, 
  event_type, 
  event_month,
  traffic_source,
  COUNT(event_type) AS no_of_events,
  FROM combined
  GROUP BY event_type, traffic_source, event_month, country
  ORDER BY event_order