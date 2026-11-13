SELECT
  customer_id,
  COUNT(*) AS rows,
  SUM(amount_cents) AS amount_sum
FROM benchmark_data
GROUP BY customer_id;
