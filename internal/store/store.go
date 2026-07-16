package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yash/dispatch/internal/circuitbreaker"
)

var ErrNotFound = errors.New("not found")
var ErrConflict = errors.New("conflict")

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Pool() *pgxpool.Pool { return s.pool }

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

type Tenant struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

func (s *Store) CreateTenant(ctx context.Context, name, apiKeyHash string) (Tenant, error) {
	var t Tenant
	err := s.pool.QueryRow(ctx, `
		INSERT INTO tenants (name, api_key_hash)
		VALUES ($1, $2)
		RETURNING id, name, created_at
	`, name, apiKeyHash).Scan(&t.ID, &t.Name, &t.CreatedAt)
	return t, err
}

func (s *Store) TenantByAPIKeyHash(ctx context.Context, hash string) (Tenant, error) {
	var t Tenant
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, created_at FROM tenants WHERE api_key_hash = $1
	`, hash).Scan(&t.ID, &t.Name, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Tenant{}, ErrNotFound
	}
	return t, err
}

type Subscription struct {
	ID                       uuid.UUID  `json:"id"`
	TenantID                 uuid.UUID  `json:"tenant_id"`
	URL                      string     `json:"url"`
	EventTypes               []string   `json:"event_types"`
	Secret                   string     `json:"-"`
	PreviousSecret           *string    `json:"-"`
	PreviousSecretExpiresAt  *time.Time `json:"-"`
	State                    string     `json:"state"`
	ConsecutiveFailures      int        `json:"consecutive_failures"`
	StateChangedAt           time.Time  `json:"state_changed_at"`
	DLQCount                 int        `json:"dlq_count"`
	CreatedAt                time.Time  `json:"created_at"`
}

func (s *Store) CreateSubscription(ctx context.Context, tenantID uuid.UUID, url string, eventTypes []string, secret string) (Subscription, error) {
	if eventTypes == nil {
		eventTypes = []string{}
	}
	var sub Subscription
	err := s.pool.QueryRow(ctx, `
		INSERT INTO subscriptions (tenant_id, url, event_types, secret)
		VALUES ($1, $2, $3, $4)
		RETURNING id, tenant_id, url, event_types, secret, previous_secret, previous_secret_expires_at,
		          state, consecutive_failures, state_changed_at, dlq_count, created_at
	`, tenantID, url, eventTypes, secret).Scan(
		&sub.ID, &sub.TenantID, &sub.URL, &sub.EventTypes, &sub.Secret,
		&sub.PreviousSecret, &sub.PreviousSecretExpiresAt,
		&sub.State, &sub.ConsecutiveFailures, &sub.StateChangedAt, &sub.DLQCount, &sub.CreatedAt,
	)
	return sub, err
}

func (s *Store) GetSubscription(ctx context.Context, tenantID, id uuid.UUID) (Subscription, error) {
	var sub Subscription
	err := s.pool.QueryRow(ctx, `
		SELECT id, tenant_id, url, event_types, secret, previous_secret, previous_secret_expires_at,
		       state, consecutive_failures, state_changed_at, dlq_count, created_at
		FROM subscriptions WHERE id = $1 AND tenant_id = $2
	`, id, tenantID).Scan(
		&sub.ID, &sub.TenantID, &sub.URL, &sub.EventTypes, &sub.Secret,
		&sub.PreviousSecret, &sub.PreviousSecretExpiresAt,
		&sub.State, &sub.ConsecutiveFailures, &sub.StateChangedAt, &sub.DLQCount, &sub.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Subscription{}, ErrNotFound
	}
	return sub, err
}

func (s *Store) ListSubscriptions(ctx context.Context, tenantID uuid.UUID, cursor *time.Time, limit int) ([]Subscription, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	var rows pgx.Rows
	var err error
	if cursor == nil {
		rows, err = s.pool.Query(ctx, `
			SELECT id, tenant_id, url, event_types, secret, previous_secret, previous_secret_expires_at,
			       state, consecutive_failures, state_changed_at, dlq_count, created_at
			FROM subscriptions WHERE tenant_id = $1
			ORDER BY created_at DESC LIMIT $2
		`, tenantID, limit)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id, tenant_id, url, event_types, secret, previous_secret, previous_secret_expires_at,
			       state, consecutive_failures, state_changed_at, dlq_count, created_at
			FROM subscriptions WHERE tenant_id = $1 AND created_at < $2
			ORDER BY created_at DESC LIMIT $3
		`, tenantID, *cursor, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Subscription
	for rows.Next() {
		var sub Subscription
		if err := rows.Scan(
			&sub.ID, &sub.TenantID, &sub.URL, &sub.EventTypes, &sub.Secret,
			&sub.PreviousSecret, &sub.PreviousSecretExpiresAt,
			&sub.State, &sub.ConsecutiveFailures, &sub.StateChangedAt, &sub.DLQCount, &sub.CreatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

func (s *Store) DeleteSubscription(ctx context.Context, tenantID, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM subscriptions WHERE id = $1 AND tenant_id = $2`, id, tenantID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) RotateSecret(ctx context.Context, tenantID, id uuid.UUID, newSecret string, gracePeriod time.Duration) (Subscription, error) {
	now := time.Now().UTC()
	expires := now.Add(gracePeriod)
	var sub Subscription
	err := s.pool.QueryRow(ctx, `
		UPDATE subscriptions
		SET previous_secret = secret,
		    previous_secret_expires_at = $1,
		    secret = $2
		WHERE id = $3 AND tenant_id = $4
		RETURNING id, tenant_id, url, event_types, secret, previous_secret, previous_secret_expires_at,
		          state, consecutive_failures, state_changed_at, dlq_count, created_at
	`, expires, newSecret, id, tenantID).Scan(
		&sub.ID, &sub.TenantID, &sub.URL, &sub.EventTypes, &sub.Secret,
		&sub.PreviousSecret, &sub.PreviousSecretExpiresAt,
		&sub.State, &sub.ConsecutiveFailures, &sub.StateChangedAt, &sub.DLQCount, &sub.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Subscription{}, ErrNotFound
	}
	return sub, err
}

func (s *Store) ActivateSubscription(ctx context.Context, tenantID, id uuid.UUID) (Subscription, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Subscription{}, err
	}
	defer tx.Rollback(ctx)

	var sub Subscription
	err = tx.QueryRow(ctx, `
		SELECT id, tenant_id, url, event_types, secret, previous_secret, previous_secret_expires_at,
		       state, consecutive_failures, state_changed_at, dlq_count, created_at
		FROM subscriptions WHERE id = $1 AND tenant_id = $2 FOR UPDATE
	`, id, tenantID).Scan(
		&sub.ID, &sub.TenantID, &sub.URL, &sub.EventTypes, &sub.Secret,
		&sub.PreviousSecret, &sub.PreviousSecretExpiresAt,
		&sub.State, &sub.ConsecutiveFailures, &sub.StateChangedAt, &sub.DLQCount, &sub.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Subscription{}, ErrNotFound
	}
	if err != nil {
		return Subscription{}, err
	}

	from := sub.State
	now := time.Now().UTC()
	err = tx.QueryRow(ctx, `
		UPDATE subscriptions
		SET state = 'active', consecutive_failures = 0, state_changed_at = $1
		WHERE id = $2
		RETURNING id, tenant_id, url, event_types, secret, previous_secret, previous_secret_expires_at,
		          state, consecutive_failures, state_changed_at, dlq_count, created_at
	`, now, id).Scan(
		&sub.ID, &sub.TenantID, &sub.URL, &sub.EventTypes, &sub.Secret,
		&sub.PreviousSecret, &sub.PreviousSecretExpiresAt,
		&sub.State, &sub.ConsecutiveFailures, &sub.StateChangedAt, &sub.DLQCount, &sub.CreatedAt,
	)
	if err != nil {
		return Subscription{}, err
	}

	if from != circuitbreaker.StateActive {
		if _, err := tx.Exec(ctx, `
			INSERT INTO subscription_state_transitions (subscription_id, from_state, to_state, reason)
			VALUES ($1, $2, 'active', 'manual_activate')
		`, id, from); err != nil {
			return Subscription{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return Subscription{}, err
	}
	return sub, nil
}

type Event struct {
	ID             uuid.UUID       `json:"id"`
	TenantID       uuid.UUID       `json:"tenant_id"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

func (s *Store) CreateEvent(ctx context.Context, id uuid.UUID, tenantID uuid.UUID, eventType string, payload json.RawMessage, idemKey *string) (Event, error) {
	var ev Event
	err := s.pool.QueryRow(ctx, `
		INSERT INTO events (id, tenant_id, event_type, payload, idempotency_key)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, tenant_id, event_type, payload, idempotency_key, created_at
	`, id, tenantID, eventType, payload, idemKey).Scan(
		&ev.ID, &ev.TenantID, &ev.EventType, &ev.Payload, &ev.IdempotencyKey, &ev.CreatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return Event{}, ErrConflict
		}
		return Event{}, err
	}
	return ev, nil
}

func (s *Store) EventByIdempotencyKey(ctx context.Context, tenantID uuid.UUID, idemKey string) (Event, error) {
	var ev Event
	err := s.pool.QueryRow(ctx, `
		SELECT id, tenant_id, event_type, payload, idempotency_key, created_at
		FROM events WHERE tenant_id = $1 AND idempotency_key = $2
	`, tenantID, idemKey).Scan(
		&ev.ID, &ev.TenantID, &ev.EventType, &ev.Payload, &ev.IdempotencyKey, &ev.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Event{}, ErrNotFound
	}
	return ev, err
}

func (s *Store) MatchingSubscriptions(ctx context.Context, tenantID uuid.UUID, eventType string) ([]Subscription, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, tenant_id, url, event_types, secret, previous_secret, previous_secret_expires_at,
		       state, consecutive_failures, state_changed_at, dlq_count, created_at
		FROM subscriptions
		WHERE tenant_id = $1
		  AND (cardinality(event_types) = 0 OR $2 = ANY(event_types))
	`, tenantID, eventType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Subscription
	for rows.Next() {
		var sub Subscription
		if err := rows.Scan(
			&sub.ID, &sub.TenantID, &sub.URL, &sub.EventTypes, &sub.Secret,
			&sub.PreviousSecret, &sub.PreviousSecretExpiresAt,
			&sub.State, &sub.ConsecutiveFailures, &sub.StateChangedAt, &sub.DLQCount, &sub.CreatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

type DeliveryAttempt struct {
	ID             int64      `json:"id"`
	EventID        uuid.UUID  `json:"event_id"`
	SubscriptionID uuid.UUID  `json:"subscription_id"`
	StatusCode     *int       `json:"status_code,omitempty"`
	Error          *string    `json:"error,omitempty"`
	LatencyMs      *int       `json:"latency_ms,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
}

func (s *Store) InsertDeliveryAttempt(ctx context.Context, eventID, subID uuid.UUID, statusCode *int, errMsg *string, latencyMs *int) (DeliveryAttempt, error) {
	var a DeliveryAttempt
	err := s.pool.QueryRow(ctx, `
		INSERT INTO delivery_attempts (event_id, subscription_id, status_code, error, latency_ms)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, event_id, subscription_id, status_code, error, latency_ms, created_at
	`, eventID, subID, statusCode, errMsg, latencyMs).Scan(
		&a.ID, &a.EventID, &a.SubscriptionID, &a.StatusCode, &a.Error, &a.LatencyMs, &a.CreatedAt,
	)
	return a, err
}

func (s *Store) ListDeliveries(ctx context.Context, tenantID, subID uuid.UUID, cursor *time.Time, limit int) ([]DeliveryAttempt, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	// Ensure subscription belongs to tenant
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM subscriptions WHERE id = $1 AND tenant_id = $2)
	`, subID, tenantID).Scan(&exists)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}

	var rows pgx.Rows
	if cursor == nil {
		rows, err = s.pool.Query(ctx, `
			SELECT id, event_id, subscription_id, status_code, error, latency_ms, created_at
			FROM delivery_attempts WHERE subscription_id = $1
			ORDER BY created_at DESC LIMIT $2
		`, subID, limit)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id, event_id, subscription_id, status_code, error, latency_ms, created_at
			FROM delivery_attempts WHERE subscription_id = $1 AND created_at < $2
			ORDER BY created_at DESC LIMIT $3
		`, subID, *cursor, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DeliveryAttempt
	for rows.Next() {
		var a DeliveryAttempt
		if err := rows.Scan(&a.ID, &a.EventID, &a.SubscriptionID, &a.StatusCode, &a.Error, &a.LatencyMs, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// RecordSuccess resets failures and may transition degraded→active on half-open probe.
func (s *Store) RecordSuccess(ctx context.Context, subID uuid.UUID, halfOpen bool) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var state string
	err = tx.QueryRow(ctx, `
		SELECT state FROM subscriptions WHERE id = $1 FOR UPDATE
	`, subID).Scan(&state)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	newState, reset, transition, reason := circuitbreaker.NextAfterSuccess(state, halfOpen)
	if reset {
		if transition {
			tag, err := tx.Exec(ctx, `
				UPDATE subscriptions
				SET state = $1, consecutive_failures = 0, state_changed_at = $2
				WHERE id = $3 AND state = $4
			`, newState, now, subID, state)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 1 {
				if _, err := tx.Exec(ctx, `
					INSERT INTO subscription_state_transitions (subscription_id, from_state, to_state, reason)
					VALUES ($1, $2, $3, $4)
				`, subID, state, newState, reason); err != nil {
					return err
				}
			}
		} else {
			_, err = tx.Exec(ctx, `
				UPDATE subscriptions SET consecutive_failures = 0 WHERE id = $1
			`, subID)
			if err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

// RecordFailure increments consecutive failures and may transition active→degraded.
func (s *Store) RecordFailure(ctx context.Context, subID uuid.UUID, halfOpen bool, cfg circuitbreaker.Config) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var state string
	var consecutive int
	var dlqCount int
	var stateChangedAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT state, consecutive_failures, dlq_count, state_changed_at
		FROM subscriptions WHERE id = $1 FOR UPDATE
	`, subID).Scan(&state, &consecutive, &dlqCount, &stateChangedAt)
	if err != nil {
		return err
	}

	newState, newFailures, transition, reason := circuitbreaker.NextAfterFailure(state, halfOpen, consecutive, dlqCount, cfg)
	now := time.Now().UTC()

	if transition {
		// Atomic transition: only if still in expected from-state.
		if newState == circuitbreaker.StateDegraded && state == circuitbreaker.StateActive {
			tag, err := tx.Exec(ctx, `
				UPDATE subscriptions
				SET consecutive_failures = $1, state = 'degraded', state_changed_at = $2
				WHERE id = $3 AND state = 'active' AND consecutive_failures >= $4 - 1
			`, newFailures, now, subID, cfg.FailureThreshold)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 1 {
				if _, err := tx.Exec(ctx, `
					INSERT INTO subscription_state_transitions (subscription_id, from_state, to_state, reason)
					VALUES ($1, 'active', 'degraded', $2)
				`, subID, reason); err != nil {
					return err
				}
			} else {
				_, err = tx.Exec(ctx, `
					UPDATE subscriptions SET consecutive_failures = $1 WHERE id = $2
				`, newFailures, subID)
				if err != nil {
					return err
				}
			}
		} else if reason == "half_open_probe_failed" {
			_, err = tx.Exec(ctx, `
				UPDATE subscriptions
				SET consecutive_failures = $1, state = 'degraded', state_changed_at = $2
				WHERE id = $3
			`, newFailures, now, subID)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO subscription_state_transitions (subscription_id, from_state, to_state, reason)
				VALUES ($1, $2, 'degraded', $3)
			`, subID, state, reason); err != nil {
				return err
			}
		} else if newState == circuitbreaker.StatePaused {
			tag, err := tx.Exec(ctx, `
				UPDATE subscriptions
				SET consecutive_failures = $1, state = 'paused', state_changed_at = $2
				WHERE id = $3 AND state <> 'paused'
			`, newFailures, now, subID)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 1 {
				if _, err := tx.Exec(ctx, `
					INSERT INTO subscription_state_transitions (subscription_id, from_state, to_state, reason)
					VALUES ($1, $2, 'paused', $3)
				`, subID, state, reason); err != nil {
					return err
				}
			}
		} else {
			_, err = tx.Exec(ctx, `
				UPDATE subscriptions SET consecutive_failures = $1 WHERE id = $2
			`, newFailures, subID)
			if err != nil {
				return err
			}
		}
	} else {
		_, err = tx.Exec(ctx, `
			UPDATE subscriptions SET consecutive_failures = $1 WHERE id = $2
		`, newFailures, subID)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (s *Store) GetSubscriptionByID(ctx context.Context, id uuid.UUID) (Subscription, error) {
	var sub Subscription
	err := s.pool.QueryRow(ctx, `
		SELECT id, tenant_id, url, event_types, secret, previous_secret, previous_secret_expires_at,
		       state, consecutive_failures, state_changed_at, dlq_count, created_at
		FROM subscriptions WHERE id = $1
	`, id).Scan(
		&sub.ID, &sub.TenantID, &sub.URL, &sub.EventTypes, &sub.Secret,
		&sub.PreviousSecret, &sub.PreviousSecretExpiresAt,
		&sub.State, &sub.ConsecutiveFailures, &sub.StateChangedAt, &sub.DLQCount, &sub.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Subscription{}, ErrNotFound
	}
	return sub, err
}

func (s *Store) GetEvent(ctx context.Context, id uuid.UUID) (Event, error) {
	var ev Event
	err := s.pool.QueryRow(ctx, `
		SELECT id, tenant_id, event_type, payload, idempotency_key, created_at
		FROM events WHERE id = $1
	`, id).Scan(&ev.ID, &ev.TenantID, &ev.EventType, &ev.Payload, &ev.IdempotencyKey, &ev.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Event{}, ErrNotFound
	}
	return ev, err
}

// ListUndeliveredEvents returns events older than age with no delivery_attempts (recovery sweep).
func (s *Store) ListUndeliveredEvents(ctx context.Context, olderThan time.Duration, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.id, e.tenant_id, e.event_type, e.payload, e.idempotency_key, e.created_at
		FROM events e
		WHERE e.created_at < now() - $1::interval
		  AND NOT EXISTS (SELECT 1 FROM delivery_attempts da WHERE da.event_id = e.id)
		ORDER BY e.created_at ASC
		LIMIT $2
	`, fmt.Sprintf("%f seconds", olderThan.Seconds()), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var ev Event
		if err := rows.Scan(&ev.ID, &ev.TenantID, &ev.EventType, &ev.Payload, &ev.IdempotencyKey, &ev.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

type DeadLetter struct {
	ID             uuid.UUID  `json:"id"`
	EventID        uuid.UUID  `json:"event_id"`
	SubscriptionID uuid.UUID  `json:"subscription_id"`
	AttemptCount   int        `json:"attempt_count"`
	LastError      *string    `json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	ReplayedAt     *time.Time `json:"replayed_at,omitempty"`
}

func (s *Store) InsertDeadLetter(ctx context.Context, eventID, subID uuid.UUID, attemptCount int, lastErr string, cfg circuitbreaker.Config) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO dead_letters (event_id, subscription_id, attempt_count, last_error)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (event_id, subscription_id) WHERE replayed_at IS NULL DO NOTHING
	`, eventID, subID, attemptCount, lastErr)
	if err != nil {
		return err
	}

	var state string
	var dlqCount int
	err = tx.QueryRow(ctx, `
		UPDATE subscriptions
		SET dlq_count = dlq_count + 1
		WHERE id = $1
		RETURNING state, dlq_count
	`, subID).Scan(&state, &dlqCount)
	if err != nil {
		return err
	}

	if dlqCount >= cfg.DLQPauseThreshold && state != circuitbreaker.StatePaused {
		now := time.Now().UTC()
		tag, err := tx.Exec(ctx, `
			UPDATE subscriptions
			SET state = 'paused', state_changed_at = $1
			WHERE id = $2 AND state <> 'paused'
		`, now, subID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			if _, err := tx.Exec(ctx, `
				INSERT INTO subscription_state_transitions (subscription_id, from_state, to_state, reason)
				VALUES ($1, $2, 'paused', 'dlq_threshold')
			`, subID, state); err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

// CountSubscriptionsByState returns counts keyed by state (active/degraded/paused).
func (s *Store) CountSubscriptionsByState(ctx context.Context) (map[string]int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT state, COUNT(*)::int FROM subscriptions GROUP BY state
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{
		circuitbreaker.StateActive:   0,
		circuitbreaker.StateDegraded: 0,
		circuitbreaker.StatePaused:   0,
	}
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return nil, err
		}
		out[state] = n
	}
	return out, rows.Err()
}

func (s *Store) ListDeadLetters(ctx context.Context, tenantID, subscriptionID uuid.UUID, cursor *time.Time, limit int) ([]DeadLetter, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM subscriptions WHERE id = $1 AND tenant_id = $2)
	`, subscriptionID, tenantID).Scan(&exists)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}

	var rows pgx.Rows
	if cursor == nil {
		rows, err = s.pool.Query(ctx, `
			SELECT id, event_id, subscription_id, attempt_count, last_error, created_at, replayed_at
			FROM dead_letters
			WHERE subscription_id = $1 AND replayed_at IS NULL
			ORDER BY created_at DESC LIMIT $2
		`, subscriptionID, limit)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id, event_id, subscription_id, attempt_count, last_error, created_at, replayed_at
			FROM dead_letters
			WHERE subscription_id = $1 AND replayed_at IS NULL AND created_at < $2
			ORDER BY created_at DESC LIMIT $3
		`, subscriptionID, *cursor, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DeadLetter
	for rows.Next() {
		var d DeadLetter
		if err := rows.Scan(&d.ID, &d.EventID, &d.SubscriptionID, &d.AttemptCount, &d.LastError, &d.CreatedAt, &d.ReplayedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// MarkDeadLetterReplayed sets replayed_at if still pending. Returns the linked event for re-produce.
func (s *Store) MarkDeadLetterReplayed(ctx context.Context, tenantID, id uuid.UUID) (Event, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Event{}, err
	}
	defer tx.Rollback(ctx)

	var eventID uuid.UUID
	var replayedAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT dl.event_id, dl.replayed_at
		FROM dead_letters dl
		JOIN subscriptions s ON s.id = dl.subscription_id
		WHERE dl.id = $1 AND s.tenant_id = $2
		FOR UPDATE OF dl
	`, id, tenantID).Scan(&eventID, &replayedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Event{}, ErrNotFound
	}
	if err != nil {
		return Event{}, err
	}
	if replayedAt != nil {
		return Event{}, ErrConflict
	}

	tag, err := tx.Exec(ctx, `
		UPDATE dead_letters SET replayed_at = now() WHERE id = $1 AND replayed_at IS NULL
	`, id)
	if err != nil {
		return Event{}, err
	}
	if tag.RowsAffected() == 0 {
		return Event{}, ErrConflict
	}

	var ev Event
	err = tx.QueryRow(ctx, `
		SELECT id, tenant_id, event_type, payload, idempotency_key, created_at
		FROM events WHERE id = $1
	`, eventID).Scan(&ev.ID, &ev.TenantID, &ev.EventType, &ev.Payload, &ev.IdempotencyKey, &ev.CreatedAt)
	if err != nil {
		return Event{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Event{}, err
	}
	return ev, nil
}

func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func FormatCursor(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func ParseCursor(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t2, err2 := time.Parse(time.RFC3339, s)
		if err2 != nil {
			return nil, fmt.Errorf("invalid cursor: %w", err)
		}
		t = t2
	}
	return &t, nil
}
