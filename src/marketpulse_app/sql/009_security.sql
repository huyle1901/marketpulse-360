CREATE SCHEMA IF NOT EXISTS marketpulse_app;
CREATE TABLE IF NOT EXISTS marketpulse_app.auth_users (
 user_id uuid PRIMARY KEY, username text UNIQUE NOT NULL, password_hash text NOT NULL,
 role text NOT NULL CHECK(role IN ('viewer','operator','admin')), enabled boolean NOT NULL DEFAULT true,
 version bigint NOT NULL DEFAULT 1, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS marketpulse_app.auth_sessions (
 token_hash text PRIMARY KEY, user_id uuid NOT NULL REFERENCES marketpulse_app.auth_users(user_id),
 expires_at timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS auth_session_expiry ON marketpulse_app.auth_sessions(expires_at);
CREATE TABLE IF NOT EXISTS marketpulse_app.login_failures (
 username_hash text PRIMARY KEY, failures integer NOT NULL, window_start timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS marketpulse_app.access_audit (
 event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, observed_at timestamptz NOT NULL DEFAULT now(),
 request_id text NOT NULL, user_id text, role text NOT NULL, route text NOT NULL,
 decision text NOT NULL, status integer);
CREATE OR REPLACE FUNCTION marketpulse_app.reject_evidence_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'immutable operational evidence'; END $$;
DROP TRIGGER IF EXISTS immutable_access_audit ON marketpulse_app.access_audit;
CREATE TRIGGER immutable_access_audit BEFORE UPDATE OR DELETE ON marketpulse_app.access_audit
FOR EACH ROW EXECUTE FUNCTION marketpulse_app.reject_evidence_change();
