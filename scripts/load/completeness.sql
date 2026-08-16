-- Completeness check after a load run.
-- Optional: psql -v tenant_id=UUID to scope counts to one tenant.
-- The invariant is "nothing aged is orphaned." Scripts fail the run if
-- orphan_count > 0 (see lib.sh assert_completeness).

\if :{?tenant_id}
\else
\set tenant_id ''
\endif

\echo '=== event vs attempt/dlq completeness ==='

SELECT
  (SELECT COUNT(*) FROM events e
    WHERE NULLIF(:'tenant_id', '') IS NULL OR e.tenant_id = CAST(NULLIF(:'tenant_id', '') AS uuid)) AS events,
  (SELECT COUNT(*) FROM delivery_attempts da
    JOIN events e ON e.id = da.event_id
    WHERE da.status_code BETWEEN 200 AND 299
      AND (NULLIF(:'tenant_id', '') IS NULL OR e.tenant_id = CAST(NULLIF(:'tenant_id', '') AS uuid))) AS successful_attempts,
  (SELECT COUNT(*) FROM delivery_attempts da
    JOIN events e ON e.id = da.event_id
    WHERE NULLIF(:'tenant_id', '') IS NULL OR e.tenant_id = CAST(NULLIF(:'tenant_id', '') AS uuid)) AS total_attempts,
  (SELECT COUNT(*) FROM dead_letters dl
    JOIN events e ON e.id = dl.event_id
    WHERE dl.replayed_at IS NULL
      AND (NULLIF(:'tenant_id', '') IS NULL OR e.tenant_id = CAST(NULLIF(:'tenant_id', '') AS uuid))) AS pending_dlq,
  (SELECT COUNT(*) FROM dead_letters dl
    JOIN events e ON e.id = dl.event_id
    WHERE NULLIF(:'tenant_id', '') IS NULL OR e.tenant_id = CAST(NULLIF(:'tenant_id', '') AS uuid)) AS total_dlq;

\echo '=== events older than 30s with zero delivery_attempts (possible loss / filter miss) ==='

SELECT e.id, e.event_type, e.created_at
FROM events e
LEFT JOIN delivery_attempts da ON da.event_id = e.id
WHERE e.created_at < NOW() - INTERVAL '30 seconds'
  AND (NULLIF(:'tenant_id', '') IS NULL OR e.tenant_id = CAST(NULLIF(:'tenant_id', '') AS uuid))
GROUP BY e.id
HAVING COUNT(da.id) = 0
ORDER BY e.created_at DESC
LIMIT 20;

\echo '=== done ==='
