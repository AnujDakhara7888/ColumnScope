SELECT
  category,
  AVG(metric) AS metric_avg,
  MIN(metric) AS metric_min,
  MAX(metric) AS metric_max
FROM benchmark_data
GROUP BY category;
