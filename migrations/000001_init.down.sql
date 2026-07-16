-- +migrate Down

DROP TABLE IF EXISTS dead_letters;
DROP TABLE IF EXISTS delivery_attempts;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS subscription_state_transitions;
DROP TABLE IF EXISTS subscriptions;
DROP TABLE IF EXISTS tenants;
