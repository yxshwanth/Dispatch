-- +migrate Up

CREATE TABLE tenants (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    api_key_hash    TEXT NOT NULL UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE subscriptions (
    id                          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id                   UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    url                         TEXT NOT NULL,
    event_types                 TEXT[] NOT NULL DEFAULT '{}',
    secret                      TEXT NOT NULL,
    previous_secret             TEXT,
    previous_secret_expires_at  TIMESTAMPTZ,
    state                       TEXT NOT NULL DEFAULT 'active'
                                CHECK (state IN ('active', 'degraded', 'paused')),
    consecutive_failures        INT NOT NULL DEFAULT 0,
    state_changed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    dlq_count                   INT NOT NULL DEFAULT 0,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_subscriptions_tenant_id ON subscriptions(tenant_id);

CREATE TABLE subscription_state_transitions (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscription_id UUID NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    from_state      TEXT NOT NULL,
    to_state        TEXT NOT NULL,
    reason          TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_state_transitions_subscription_id
    ON subscription_state_transitions(subscription_id);

CREATE TABLE events (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_type       TEXT NOT NULL,
    payload          JSONB NOT NULL,
    idempotency_key  TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_events_tenant_idempotency
    ON events(tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_events_tenant_id ON events(tenant_id);

CREATE TABLE delivery_attempts (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id         UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    subscription_id  UUID NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    status_code      INT,
    error            TEXT,
    latency_ms       INT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_delivery_attempts_subscription_id
    ON delivery_attempts(subscription_id, created_at DESC);
CREATE INDEX idx_delivery_attempts_event_id
    ON delivery_attempts(event_id);

CREATE TABLE dead_letters (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id         UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    subscription_id  UUID NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    attempt_count    INT NOT NULL,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at      TIMESTAMPTZ
);

CREATE INDEX idx_dead_letters_pending
    ON dead_letters(subscription_id, created_at DESC)
    WHERE replayed_at IS NULL;

CREATE UNIQUE INDEX idx_dead_letters_unique_pending
    ON dead_letters(event_id, subscription_id)
    WHERE replayed_at IS NULL;
