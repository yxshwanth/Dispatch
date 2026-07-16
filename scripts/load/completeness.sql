-- Completeness check after a load run.
-- Counts are volume-wide (Compose Postgres persists across sessions), not scoped
-- to this vegeta batch — the invariant is "nothing aged is orphaned," not
-- "events == this run's request count."

\echo '=== event vs attempt/dlq completeness ==='

SELECT
  (SELECT COUNT(*) FROM events) AS events,
  (SELECT COUNT(*) FROM delivery_attempts WHERE status_code BETWEEN 200 AND 299) AS successful_attempts,
  (SELECT COUNT(*) FROM delivery_attempts) AS total_attempts,
  (SELECT COUNT(*) FROM dead_letters WHERE replayed_at IS NULL) AS pending_dlq,
  (SELECT COUNT(*) FROM dead_letters) AS total_dlq;

\echo '=== events older than 30s with zero delivery_attempts (possible loss / filter miss) ==='

SELECT e.id, e.event_type, e.created_at
FROM events e
LEFT JOIN delivery_attempts da ON da.event_id = e.id
WHERE e.created_at < NOW() - INTERVAL '30 seconds'
GROUP BY e.id
HAVING COUNT(da.id) = 0
ORDER BY e.created_at DESC
LIMIT 20;

\echo '=== done ==='
