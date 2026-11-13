SELECT
  region,
  category,
  COUNT(*) AS rows,
  SUM(amount_cents) AS amount_sum
FROM benchmark_data
WHERE selectivity_key < 1000 AND active = TRUE
GROUP BY region, category;
