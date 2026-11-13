SELECT
  COUNT(*) AS rows,
  COUNT(amount_cents) AS amount_count,
  SUM(amount_cents) AS amount_sum
FROM benchmark_data;
