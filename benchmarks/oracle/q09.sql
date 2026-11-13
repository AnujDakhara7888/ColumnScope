SELECT
  region,
  COUNT(*) AS rows,
  SUM(amount_cents) AS amount_sum
FROM benchmark_data
GROUP BY region;
