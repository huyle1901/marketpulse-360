CREATE SCHEMA IF NOT EXISTS marketpulse_app;
CREATE TABLE IF NOT EXISTS marketpulse_app.segments (
    segment_id uuid PRIMARY KEY,
    version bigint NOT NULL CHECK(version > 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS marketpulse_app.segment_revisions (
    segment_id uuid NOT NULL REFERENCES marketpulse_app.segments(segment_id),
    version bigint NOT NULL CHECK(version > 0),
    name text NOT NULL CHECK(length(name) BETWEEN 1 AND 80 AND name=btrim(name)),
    rule jsonb NOT NULL CHECK(jsonb_typeof(rule)='object'),
    rule_digest text NOT NULL CHECK(rule_digest ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(segment_id,version)
);
CREATE OR REPLACE FUNCTION marketpulse_app.reject_segment_revision_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'segment revisions are immutable' USING ERRCODE='23514'; END $$;
DROP TRIGGER IF EXISTS immutable_segment_revision ON marketpulse_app.segment_revisions;
CREATE TRIGGER immutable_segment_revision BEFORE UPDATE OR DELETE ON marketpulse_app.segment_revisions
FOR EACH ROW EXECUTE FUNCTION marketpulse_app.reject_segment_revision_mutation();
