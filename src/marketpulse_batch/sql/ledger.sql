CREATE SCHEMA IF NOT EXISTS marketpulse_ops;
CREATE TABLE IF NOT EXISTS marketpulse_ops.preparations (
    identity text PRIMARY KEY CHECK(identity ~ '^[0-9a-f]{64}$'),
    receipt jsonb NOT NULL,
    receipt_uri text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS marketpulse_ops.attempts (
    identity text NOT NULL,
    stage text NOT NULL CHECK(stage IN ('preparation','copy','rebuild')),
    attempt integer NOT NULL CHECK(attempt > 0),
    PRIMARY KEY(identity,stage)
);
CREATE TABLE IF NOT EXISTS marketpulse_ops.publications (
    generation bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    identity text NOT NULL UNIQUE REFERENCES marketpulse_ops.preparations(identity),
    database_name text NOT NULL,
    logical_day date NOT NULL,
    proof_digest text NOT NULL CHECK(proof_digest ~ '^[0-9a-f]{64}$'),
    reserved_at timestamptz NOT NULL DEFAULT now(),
    acknowledged_at timestamptz
);
