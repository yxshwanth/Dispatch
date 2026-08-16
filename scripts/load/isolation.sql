-- Isolation proof for one tenant with a healthy (2xx) and failing (5xx) subscription.
-- psql -v tenant_id=UUID

\echo '=== per-subscription attempts / p99 / state ==='

SELECT
  s.id AS subscription_id,
  s.url,
  s.state,
  s.consecutive_failures,
  s.dlq_count,
  COUNT(da.id) AS attempts,
  COUNT(da.id) FILTER (WHERE da.status_code BETWEEN 200 AND 299) AS ok,
  COUNT(da.id) FILTER (WHERE da.status_code >= 500) AS fail_5xx,
  ROUND(
    (percentile_cont(0.99) WITHIN GROUP (ORDER BY da.latency_ms)
      FILTER (WHERE da.status_code BETWEEN 200 AND 299))::numeric,
    2
  ) AS p99_ok_ms,
  ROUND(
    (percentile_cont(0.50) WITHIN GROUP (ORDER BY da.latency_ms)
      FILTER (WHERE da.status_code BETWEEN 200 AND 299))::numeric,
    2
  ) AS p50_ok_ms
FROM subscriptions s
LEFT JOIN delivery_attempts da ON da.subscription_id = s.id
WHERE s.tenant_id = :'tenant_id'::uuid
GROUP BY s.id, s.url, s.state, s.consecutive_failures, s.dlq_count
ORDER BY s.url;

\echo '=== tenant event / dlq counts ==='

SELECT
  (SELECT COUNT(*) FROM events WHERE tenant_id = :'tenant_id'::uuid) AS events,
  (SELECT COUNT(*) FROM dead_letters dl
     JOIN subscriptions s ON s.id = dl.subscription_id
    WHERE s.tenant_id = :'tenant_id'::uuid AND dl.replayed_at IS NULL) AS pending_dlq,
  (SELECT COUNT(*) FROM dead_letters dl
     JOIN subscriptions s ON s.id = dl.subscription_id
    WHERE s.tenant_id = :'tenant_id'::uuid) AS total_dlq;

\echo '=== done ==='
