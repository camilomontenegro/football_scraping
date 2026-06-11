-- Qualifiers JSON de WhoScored en fact_events (match centre /live)
-- Uso: psql -U postgres -d football_db -f db/migrations/add_event_qualifiers.sql

ALTER TABLE fact_events
    ADD COLUMN IF NOT EXISTS qualifiers JSONB,
    ADD COLUMN IF NOT EXISTS whoscored_event_id BIGINT;

CREATE INDEX IF NOT EXISTS idx_fact_events_qualifiers
    ON fact_events USING GIN (qualifiers);

CREATE INDEX IF NOT EXISTS idx_fact_events_ws_event_id
    ON fact_events (whoscored_event_id)
    WHERE whoscored_event_id IS NOT NULL;
