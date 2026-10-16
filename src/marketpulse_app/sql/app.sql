CREATE SCHEMA IF NOT EXISTS marketpulse_app;
CREATE TABLE IF NOT EXISTS marketpulse_app.campaign_settings (
    campaign_id text PRIMARY KEY,
    market text CHECK(market ~ '^[A-Z]{2}$'),
    currency text NOT NULL CHECK(currency ~ '^[A-Z]{3}$'),
    period_start date NOT NULL, period_end date NOT NULL,
    budget_minor bigint NOT NULL CHECK(budget_minor > 0),
    version bigint NOT NULL CHECK(version > 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK(period_end > period_start AND period_end - period_start <= 366)
);
CREATE TABLE IF NOT EXISTS marketpulse_app.alert_rules (
    rule_id text PRIMARY KEY, campaign_id text NOT NULL,
    kind text NOT NULL CHECK(kind IN ('budget_overrun','spend_spike','refund_share')),
    threshold text NOT NULL, enabled boolean NOT NULL,
    version bigint NOT NULL CHECK(version > 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS marketpulse_app.alert_evaluations (
    evaluation_key text PRIMARY KEY CHECK(evaluation_key ~ '^[0-9a-f]{64}$'),
    evidence jsonb NOT NULL,
    evaluated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS marketpulse_app.alert_occurrences (
    evaluation_key text PRIMARY KEY REFERENCES marketpulse_app.alert_evaluations(evaluation_key),
    created_at timestamptz NOT NULL DEFAULT now()
);
