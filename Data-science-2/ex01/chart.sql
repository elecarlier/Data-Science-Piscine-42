SELECT event_time, event_type, price
FROM customers
WHERE event_type = 'purchase'
  AND event_time >= '2022-10-01'
  AND event_time < '2023-03-01';
