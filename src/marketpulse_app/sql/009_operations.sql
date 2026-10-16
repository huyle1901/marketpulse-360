CREATE TABLE IF NOT EXISTS marketpulse_app.quality_reports (
 report_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 observed_at timestamptz NOT NULL DEFAULT now(), report jsonb NOT NULL);
DROP TRIGGER IF EXISTS immutable_quality_reports ON marketpulse_app.quality_reports;
CREATE TRIGGER immutable_quality_reports BEFORE UPDATE OR DELETE ON marketpulse_app.quality_reports
FOR EACH ROW EXECUTE FUNCTION marketpulse_app.reject_evidence_change();
CREATE TABLE IF NOT EXISTS marketpulse_app.ops_state (singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),probe_active boolean NOT NULL DEFAULT false);
INSERT INTO marketpulse_app.ops_state(singleton) VALUES(true) ON CONFLICT DO NOTHING;
