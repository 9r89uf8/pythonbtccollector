BEGIN;
-- Optional ghost forecasts: one complete immutable decision, versioned outcomes.
-- JSON is exact text (Decimal prices are strings); hashes cover the exported bytes.
CREATE TABLE IF NOT EXISTS ghost_twap_audit (
    run_id TEXT COLLATE "C" NOT NULL CHECK (length(run_id) BETWEEN 1 AND 128),
    decision_id TEXT COLLATE "C" NOT NULL CHECK (length(decision_id) BETWEEN 1 AND 128),
    decision_wall_ns BIGINT NOT NULL CHECK (decision_wall_ns >= 0),
    created_ms BIGINT NOT NULL CHECK (created_ms >= 0),
    frozen_json TEXT NOT NULL CHECK (jsonb_typeof(frozen_json::jsonb) = 'object'),
    frozen_sha256 TEXT NOT NULL CHECK (frozen_sha256 ~ '^[0-9a-f]{64}$'),
    target_source_timestamps_ms BIGINT[] NOT NULL DEFAULT '{}'
        CHECK (cardinality(target_source_timestamps_ms) <= 6),
    state_json TEXT NOT NULL CHECK (jsonb_typeof(state_json::jsonb) = 'object'),
    state_sha256 TEXT NOT NULL CHECK (state_sha256 ~ '^[0-9a-f]{64}$'),
    version BIGINT NOT NULL CHECK (version >= 0),
    terminal BOOLEAN NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    verified_export_sha256 TEXT,
    verified_external_location TEXT,
    verified_version BIGINT,
    verified_frozen_sha256 TEXT,
    verified_state_sha256 TEXT,
    verified_at TIMESTAMPTZ,
    PRIMARY KEY (run_id, decision_id),
    CHECK (octet_length(frozen_json) + octet_length(state_json) <= 131072),
    CHECK (
        (verified_export_sha256 IS NULL AND verified_external_location IS NULL
         AND verified_version IS NULL AND verified_frozen_sha256 IS NULL
         AND verified_state_sha256 IS NULL AND verified_at IS NULL)
        OR
        (terminal AND verified_export_sha256 IS NOT NULL
         AND verified_export_sha256 ~ '^[0-9a-f]{64}$'
         AND verified_external_location IS NOT NULL
         AND length(verified_external_location) BETWEEN 1 AND 2048
         AND verified_version IS NOT NULL AND verified_version = version
         AND verified_frozen_sha256 IS NOT NULL AND verified_frozen_sha256 = frozen_sha256
         AND verified_state_sha256 IS NOT NULL AND verified_state_sha256 = state_sha256
         AND verified_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS ghost_twap_audit_incomplete_idx
    ON ghost_twap_audit (run_id, decision_id) WHERE NOT terminal;
CREATE INDEX IF NOT EXISTS ghost_twap_audit_expiry_idx
    ON ghost_twap_audit (created_ms, run_id, decision_id)
    WHERE terminal AND verified_version IS NOT NULL;
CREATE INDEX IF NOT EXISTS ghost_twap_audit_archive_idx
    ON ghost_twap_audit (created_ms, run_id, decision_id)
    WHERE terminal AND verified_version IS NULL;
CREATE INDEX IF NOT EXISTS ghost_twap_audit_targets_idx
    ON ghost_twap_audit USING GIN (target_source_timestamps_ms) WHERE terminal;

CREATE OR REPLACE FUNCTION ghost_twap_audit_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    target_key TEXT;
    old_target JSONB;
    new_target JSONB;
    old_state JSONB;
    new_state JSONB;
    old_publication JSONB;
    new_publication JSONB;
    evidence_field TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        -- Continuous compaction is a distinct atomic path. Legacy evidence still
        -- requires its original external-export attestation and 96-hour age.
        IF OLD.terminal AND OLD.frozen_json::jsonb #> '{runtime_policy,continuous}' = 'true'::jsonb
           AND EXISTS (SELECT 1 FROM public.ghost_twap_compact c
             WHERE c.run_id=OLD.run_id AND c.decision_id=OLD.decision_id
               AND c.version=OLD.version AND c.frozen_sha256=OLD.frozen_sha256
               AND c.state_sha256=OLD.state_sha256 AND c.summary_revision=c.compact_revision)
        THEN
            RETURN OLD;
        END IF;
        IF NOT OLD.terminal OR OLD.verified_version IS NULL
           OR OLD.verified_version <> OLD.version
           OR OLD.verified_frozen_sha256 IS DISTINCT FROM OLD.frozen_sha256
           OR OLD.verified_state_sha256 IS DISTINCT FROM OLD.state_sha256
           OR OLD.created_ms > (extract(epoch FROM clock_timestamp()) * 1000)::bigint - 345600000
        THEN
            RAISE EXCEPTION 'ghost audit deletion requires age96h and current verified external export';
        END IF;
        RETURN OLD;
    END IF;
    NEW.frozen_sha256 := encode(sha256(convert_to(NEW.frozen_json, 'UTF8')), 'hex');
    NEW.state_sha256 := encode(sha256(convert_to(NEW.state_json, 'UTF8')), 'hex');
    IF TG_OP = 'INSERT' THEN
        -- Insertion alone never establishes a verified external export.
        NEW.verified_export_sha256 := NULL;
        NEW.verified_external_location := NULL;
        NEW.verified_version := NULL;
        NEW.verified_frozen_sha256 := NULL;
        NEW.verified_state_sha256 := NULL;
        NEW.verified_at := NULL;
        RETURN NEW;
    END IF;
    IF ROW(NEW.run_id, NEW.decision_id, NEW.decision_wall_ns, NEW.created_ms,
           NEW.frozen_json, NEW.target_source_timestamps_ms)
       IS DISTINCT FROM
       ROW(OLD.run_id, OLD.decision_id, OLD.decision_wall_ns, OLD.created_ms,
           OLD.frozen_json, OLD.target_source_timestamps_ms) THEN
        RAISE EXCEPTION 'ghost decision inputs are immutable';
    END IF;
    IF NEW.version < OLD.version OR (OLD.terminal AND NOT NEW.terminal) THEN
        RAISE EXCEPTION 'ghost audit state cannot regress';
    END IF;
    IF NEW.version = OLD.version AND
       ROW(NEW.state_json, NEW.terminal) IS DISTINCT FROM ROW(OLD.state_json, OLD.terminal) THEN
        RAISE EXCEPTION 'ghost audit state mutation requires a new version';
    END IF;
    old_state := OLD.state_json::jsonb;
    new_state := NEW.state_json::jsonb;
    FOREACH evidence_field IN ARRAY ARRAY['computation_completed_wall_ns', 'computation_completed_monotonic_ns']
    LOOP
        IF old_state ? evidence_field AND
           (new_state -> evidence_field) IS DISTINCT FROM (old_state -> evidence_field) THEN
            RAISE EXCEPTION 'ghost first computation clock is immutable';
        END IF;
    END LOOP;
    old_publication := COALESCE(old_state -> 'publication', '{}'::jsonb);
    new_publication := COALESCE(new_state -> 'publication', '{}'::jsonb);
    IF jsonb_typeof(old_publication) <> 'object' OR jsonb_typeof(new_publication) <> 'object' THEN
        RAISE EXCEPTION 'ghost publication evidence must be an object';
    END IF;
    FOREACH evidence_field IN ARRAY ARRAY['intent_wall_ns', 'intent_monotonic_ns',
        'attempt_wall_ns', 'attempt_monotonic_ns', 'ack_wall_ns', 'ack_monotonic_ns',
        'failure_wall_ns', 'failure_monotonic_ns']
    LOOP
        IF old_publication ? evidence_field AND
           (new_publication -> evidence_field) IS DISTINCT FROM (old_publication -> evidence_field) THEN
            RAISE EXCEPTION 'ghost first publication clock is immutable';
        END IF;
    END LOOP;
    IF old_publication ->> 'status' NOT IN ('reserved', 'intent', 'attempting') AND
       (new_publication -> 'status') IS DISTINCT FROM (old_publication -> 'status') THEN
        RAISE EXCEPTION 'ghost terminal publication outcome is immutable';
    END IF;
    IF old_publication ? 'attempt_monotonic_ns' AND old_publication ? 'payload_json' AND
       (new_publication -> 'payload_json') IS DISTINCT FROM (old_publication -> 'payload_json') THEN
        RAISE EXCEPTION 'ghost attempted publication payload is immutable';
    END IF;
    FOR target_key, old_target IN
        SELECT key, value FROM jsonb_each(COALESCE(old_state -> 'targets', '{}'::jsonb))
    LOOP
        new_target := new_state -> 'targets' -> target_key;
        FOREACH evidence_field IN ARRAY ARRAY['horizon', 'target_source_timestamp_ms']
        LOOP
            IF old_target ? evidence_field AND
               (new_target -> evidence_field) IS DISTINCT FROM (old_target -> evidence_field) THEN
                RAISE EXCEPTION 'ghost target identity is immutable';
            END IF;
        END LOOP;
        IF old_target -> 'first_event' IS NOT NULL AND old_target -> 'first_event' <> 'null'::jsonb THEN
            IF new_target -> 'first_event' IS DISTINCT FROM old_target -> 'first_event'
               OR new_target -> 'status' IS DISTINCT FROM old_target -> 'status' THEN
                RAISE EXCEPTION 'ghost first target match/status is immutable';
            END IF;
        ELSIF old_target ->> 'status' <> 'pending' AND
              (new_target -> 'status') IS DISTINCT FROM (old_target -> 'status') THEN
            RAISE EXCEPTION 'ghost terminal target status is immutable';
        END IF;
        FOREACH evidence_field IN ARRAY ARRAY['first_late_event', 'first_conflicting_event',
            'error', 'error_bps', 'persistence_error', 'eta_error_ns', 'clock_anomaly']
        LOOP
            IF old_target ? evidence_field AND old_target -> evidence_field <> 'null'::jsonb AND
               (new_target -> evidence_field) IS DISTINCT FROM (old_target -> evidence_field) THEN
                RAISE EXCEPTION 'ghost first target result evidence is immutable';
            END IF;
        END LOOP;
        FOREACH evidence_field IN ARRAY ARRAY['conflicted', 'late_missing']
        LOOP
            IF old_target -> evidence_field = 'true'::jsonb AND
               (new_target -> evidence_field) IS DISTINCT FROM 'true'::jsonb THEN
                RAISE EXCEPTION 'ghost observed target flag cannot be cleared';
            END IF;
        END LOOP;
    END LOOP;
    IF NEW.version > OLD.version THEN
        NEW.updated_at := clock_timestamp();
        NEW.verified_export_sha256 := NULL;
        NEW.verified_external_location := NULL;
        NEW.verified_version := NULL;
        NEW.verified_frozen_sha256 := NULL;
        NEW.verified_state_sha256 := NULL;
        NEW.verified_at := NULL;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS ghost_twap_audit_guard_trigger ON ghost_twap_audit;
CREATE TRIGGER ghost_twap_audit_guard_trigger
    BEFORE INSERT OR UPDATE OR DELETE ON ghost_twap_audit
    FOR EACH ROW EXECUTE FUNCTION ghost_twap_audit_guard();

-- Continuous ghost retention. These tables are runtime storage, not the
-- similarly named disposable research experiment. Original canaries stay in
-- ghost_twap_audit and never enter this automatic compact/summary path.
CREATE TABLE IF NOT EXISTS ghost_twap_compact (
    run_id TEXT COLLATE "C" NOT NULL CHECK (length(run_id) BETWEEN 1 AND 128),
    decision_id TEXT COLLATE "C" NOT NULL CHECK (length(decision_id) BETWEEN 1 AND 128),
    decision_wall_ns BIGINT NOT NULL CHECK (decision_wall_ns>=0),
    created_ms BIGINT NOT NULL CHECK (created_ms=decision_wall_ns/1000000),
    version BIGINT NOT NULL CHECK (version>=0),
    frozen_sha256 TEXT NOT NULL CHECK (frozen_sha256 ~ '^[0-9a-f]{64}$'),
    state_sha256 TEXT NOT NULL CHECK (state_sha256 ~ '^[0-9a-f]{64}$'),
    target_source_timestamps_ms BIGINT[] NOT NULL CHECK (cardinality(target_source_timestamps_ms)<=6),
    hour_start_ms BIGINT NOT NULL CHECK (hour_start_ms=(created_ms/3600000)*3600000),
    body_json TEXT NOT NULL CHECK (jsonb_typeof(body_json::jsonb)='object' AND octet_length(body_json)<=131072),
    body_sha256 TEXT NOT NULL CHECK (body_sha256 ~ '^[0-9a-f]{64}$'),
    compact_revision BIGINT NOT NULL DEFAULT 0 CHECK (compact_revision>=0),
    summary_revision BIGINT NOT NULL CHECK (summary_revision=compact_revision),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(run_id,decision_id)
);
CREATE INDEX IF NOT EXISTS ghost_twap_compact_age_idx ON ghost_twap_compact(created_ms,run_id,decision_id);
CREATE INDEX IF NOT EXISTS ghost_twap_compact_targets_idx ON ghost_twap_compact USING GIN(target_source_timestamps_ms);
CREATE INDEX IF NOT EXISTS ghost_twap_audit_created_idx ON ghost_twap_audit(created_ms);
CREATE INDEX IF NOT EXISTS ghost_twap_audit_continuous_idx ON ghost_twap_audit(created_ms,run_id,decision_id)
    WHERE terminal AND (frozen_json::jsonb #> '{runtime_policy,continuous}')='true'::jsonb;
-- The watermark includes pending rows too. Its index must not require terminal.
CREATE INDEX IF NOT EXISTS ghost_twap_audit_continuous_watermark_idx ON ghost_twap_audit(created_ms)
    WHERE (frozen_json::jsonb #> '{runtime_policy,continuous}')='true'::jsonb;

CREATE TABLE IF NOT EXISTS ghost_twap_accuracy_hourly (
    hour_start_ms BIGINT PRIMARY KEY CHECK (hour_start_ms>=0 AND hour_start_ms%3600000=0),
    body_json TEXT NOT NULL CHECK (jsonb_typeof(body_json::jsonb)='object' AND octet_length(body_json)<=2097152),
    body_sha256 TEXT NOT NULL CHECK (body_sha256 ~ '^[0-9a-f]{64}$'),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS ghost_twap_feed_health (
    run_id TEXT COLLATE "C" NOT NULL CHECK (length(run_id) BETWEEN 1 AND 128),
    hour_start_ms BIGINT NOT NULL CHECK (hour_start_ms>=0 AND hour_start_ms%3600000=0),
    revision BIGINT NOT NULL CHECK (revision>=0),
    body_json TEXT NOT NULL CHECK (jsonb_typeof(body_json::jsonb)='object' AND octet_length(body_json)<=131072),
    body_sha256 TEXT NOT NULL CHECK (body_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY(run_id,hour_start_ms)
);
CREATE INDEX IF NOT EXISTS ghost_twap_feed_health_age_idx ON ghost_twap_feed_health(hour_start_ms,run_id);
CREATE TABLE IF NOT EXISTS ghost_twap_retention_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    active_rows BIGINT NOT NULL CHECK (active_rows>=0),
    incomplete_rows BIGINT NOT NULL CHECK (incomplete_rows>=0),
    compact_rows BIGINT NOT NULL CHECK (compact_rows>=0),
    hourly_rows BIGINT NOT NULL CHECK (hourly_rows>=0),
    feed_rows BIGINT NOT NULL CHECK (feed_rows>=0),
    expired_before_ms BIGINT NOT NULL DEFAULT 0 CHECK (expired_before_ms>=0),
    expired_recoveries BIGINT NOT NULL DEFAULT 0 CHECK (expired_recoveries>=0),
    baseline_json TEXT CHECK (baseline_json IS NULL OR (jsonb_typeof(baseline_json::jsonb)='object' AND octet_length(baseline_json)<=2097152)),
    warning_json TEXT CHECK (warning_json IS NULL OR (jsonb_typeof(warning_json::jsonb)='object' AND octet_length(warning_json)<=2097152))
);
-- One migration-time seed, protected against concurrent audit writes. Later
-- initialization/guards read exact transactional counters, never count history.
LOCK TABLE ghost_twap_audit,ghost_twap_compact,ghost_twap_accuracy_hourly,ghost_twap_feed_health IN SHARE ROW EXCLUSIVE MODE;
INSERT INTO ghost_twap_retention_state(singleton,active_rows,incomplete_rows,compact_rows,hourly_rows,feed_rows)
    SELECT true,(SELECT count(*) FROM ghost_twap_audit),(SELECT count(*) FROM ghost_twap_audit WHERE NOT terminal),
      (SELECT count(*) FROM ghost_twap_compact),(SELECT count(*) FROM ghost_twap_accuracy_hourly),(SELECT count(*) FROM ghost_twap_feed_health)
    WHERE NOT EXISTS(SELECT 1 FROM ghost_twap_retention_state WHERE singleton)
    ON CONFLICT(singleton) DO NOTHING;

CREATE OR REPLACE FUNCTION ghost_twap_retention_count() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE delta BIGINT; pending_delta BIGINT;
BEGIN
    delta := CASE WHEN TG_OP='INSERT' THEN 1 WHEN TG_OP='DELETE' THEN -1 ELSE 0 END;
    IF TG_TABLE_NAME='ghost_twap_audit' THEN
        pending_delta := CASE WHEN TG_OP='INSERT' THEN CASE WHEN NEW.terminal THEN 0 ELSE 1 END
          WHEN TG_OP='DELETE' THEN CASE WHEN OLD.terminal THEN 0 ELSE -1 END
          ELSE CASE WHEN NEW.terminal=OLD.terminal THEN 0 WHEN NEW.terminal THEN -1 ELSE 1 END END;
        IF delta<>0 OR pending_delta<>0 THEN
            UPDATE public.ghost_twap_retention_state SET active_rows=active_rows+delta,incomplete_rows=incomplete_rows+pending_delta WHERE singleton;
        END IF;
    ELSIF TG_TABLE_NAME='ghost_twap_compact' THEN
        UPDATE public.ghost_twap_retention_state SET compact_rows=compact_rows+delta WHERE singleton;
    ELSIF TG_TABLE_NAME='ghost_twap_accuracy_hourly' THEN
        UPDATE public.ghost_twap_retention_state SET hourly_rows=hourly_rows+delta WHERE singleton;
    ELSIF TG_TABLE_NAME='ghost_twap_feed_health' THEN
        UPDATE public.ghost_twap_retention_state SET feed_rows=feed_rows+delta WHERE singleton;
    END IF;
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS ghost_twap_retention_count_trigger ON ghost_twap_audit;
CREATE TRIGGER ghost_twap_retention_count_trigger AFTER INSERT OR DELETE OR UPDATE OF terminal ON ghost_twap_audit
    FOR EACH ROW EXECUTE FUNCTION ghost_twap_retention_count();
DROP TRIGGER IF EXISTS ghost_twap_retention_count_trigger ON ghost_twap_compact;
CREATE TRIGGER ghost_twap_retention_count_trigger AFTER INSERT OR DELETE ON ghost_twap_compact
    FOR EACH ROW EXECUTE FUNCTION ghost_twap_retention_count();
DROP TRIGGER IF EXISTS ghost_twap_retention_count_trigger ON ghost_twap_accuracy_hourly;
CREATE TRIGGER ghost_twap_retention_count_trigger AFTER INSERT OR DELETE ON ghost_twap_accuracy_hourly
    FOR EACH ROW EXECUTE FUNCTION ghost_twap_retention_count();
DROP TRIGGER IF EXISTS ghost_twap_retention_count_trigger ON ghost_twap_feed_health;
CREATE TRIGGER ghost_twap_retention_count_trigger AFTER INSERT OR DELETE ON ghost_twap_feed_health
    FOR EACH ROW EXECUTE FUNCTION ghost_twap_retention_count();

CREATE OR REPLACE FUNCTION ghost_twap_retention_insert_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE floor_ms BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(NEW.run_id||chr(31)||NEW.decision_id,917));
    IF EXISTS(SELECT 1 FROM public.ghost_twap_compact WHERE run_id=NEW.run_id AND decision_id=NEW.decision_id) THEN
        RAISE EXCEPTION 'compacted ghost identity cannot be reinserted';
    END IF;
    IF NEW.frozen_json::jsonb #> '{runtime_policy,continuous}' = 'true'::jsonb THEN
        SELECT greatest(expired_before_ms,(extract(epoch FROM clock_timestamp())*1000)::bigint-604800000)
            INTO STRICT floor_ms FROM public.ghost_twap_retention_state WHERE singleton;
        IF NEW.created_ms<>NEW.decision_wall_ns/1000000 OR NEW.created_ms<=floor_ms THEN
            RAISE EXCEPTION 'expired or inconsistent continuous ghost decision cannot be inserted';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS ghost_twap_retention_insert_trigger ON ghost_twap_audit;
CREATE TRIGGER ghost_twap_retention_insert_trigger BEFORE INSERT ON ghost_twap_audit
    FOR EACH ROW EXECUTE FUNCTION ghost_twap_retention_insert_guard();

CREATE OR REPLACE FUNCTION ghost_twap_compact_delete_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
BEGIN
    IF OLD.created_ms>(extract(epoch FROM clock_timestamp())*1000)::bigint-604800000
       OR OLD.summary_revision<>OLD.compact_revision
       OR NOT EXISTS(SELECT 1 FROM public.ghost_twap_accuracy_hourly WHERE hour_start_ms=OLD.hour_start_ms) THEN
        RAISE EXCEPTION 'compact deletion requires seven-day age and committed summary';
    END IF;
    RETURN OLD;
END;
$$;
DROP TRIGGER IF EXISTS ghost_twap_compact_delete_trigger ON ghost_twap_compact;
CREATE TRIGGER ghost_twap_compact_delete_trigger BEFORE DELETE ON ghost_twap_compact
    FOR EACH ROW EXECUTE FUNCTION ghost_twap_compact_delete_guard();

-- These functions are the writer's only access to retained compact/summary
-- mutations. Python verifies arithmetic; SQL binds that pair to the exact
-- locked active version and makes deletion inseparable from both writes.
CREATE OR REPLACE FUNCTION ghost_twap_compact_event(e JSONB) RETURNS JSONB
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,public AS $$
    SELECT CASE WHEN e IS NULL OR e='null'::jsonb THEN 'null'::jsonb ELSE jsonb_build_object(
      'value',(e->>'value')::numeric(38,18)::text,
      'source_timestamp_ms',(e->>'source_timestamp_ms')::bigint,
      'received_wall_ns',(e->>'received_wall_ns')::bigint,
      'received_monotonic_ns',(e->>'received_monotonic_ns')::bigint,
      'sequence',(e->>'sequence')::bigint,'event_id',e->'event_id','window_s',e->'window_s') END;
$$;
CREATE OR REPLACE FUNCTION ghost_twap_compact_expected(f JSONB,s JSONB,p_version BIGINT,p_created BIGINT,p_frozen TEXT,p_state TEXT)
RETURNS JSONB LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,public AS $$
DECLARE pub JSONB; wire JSONB; selection JSONB; d JSONB; forecasts JSONB:='[]'::jsonb;
    fc JSONB; target JSONB; h TEXT; attempted BOOLEAN; stage TEXT; clock_name TEXT;
BEGIN
    pub:=s->'publication'; wire:=(pub->>'payload_json')::jsonb; selection:=wire->'publication_eligibility';
    attempted:=pub->>'attempt_monotonic_ns' IS NOT NULL;
    d:=jsonb_build_object(
      'run_id',f->'run_id','decision_id',f->'decision_id','created_ms',p_created,
      'decision_wall_ns',(f->>'decision_wall_ns')::bigint,'decision_monotonic_ns',(f->>'decision_monotonic_ns')::bigint,
      'record_version',p_version,'frozen_sha256',p_frozen,'state_sha256',p_state,
      'attempted_payload_sha256',CASE WHEN attempted THEN encode(sha256(convert_to(pub->>'payload_json','UTF8')),'hex') ELSE NULL END,
      'model_version',f->'model_version','runtime_version',f->'runtime_version','contract_version',f->'contract_version','policy',f->'policy',
      'publication_eligibility_policy',f->'publication_eligibility_policy',
      'campaign_start_ms',CASE WHEN f ? 'campaign_start_ms' THEN f->'campaign_start_ms' ELSE f #> '{runtime_policy,canary_start_ms}' END,
      'current_spot',public.ghost_twap_compact_event(f->'current_spot'),'current_twap',public.ghost_twap_compact_event(f->'current_twap'),
      'included_sequence',(f->>'included_sequence')::bigint,
      'computation_completed_wall_ns',(s->>'computation_completed_wall_ns')::bigint,
      'computation_completed_monotonic_ns',(s->>'computation_completed_monotonic_ns')::bigint,
      'valid_until_wall_ns',(f->>'valid_until_wall_ns')::bigint,'publication_status',pub->'status',
      'eligibility_checked_wall_ns',(selection->>'checked_wall_ns')::bigint,
      'eligibility_checked_monotonic_ns',(selection->>'checked_monotonic_ns')::bigint,
      'global_reasons',f->'reasons','causality_invalid',coalesce((s->>'causality_invalid')::boolean,false),
      'shutdown',s->'shutdown','restart_reconciled',coalesce((s->>'restart_reconciled')::boolean,false),
      'publication_restart_outcome',pub->'restart_outcome','gap_count',f->'gap_count',
      'spot_reconnect_status',f #> '{spot_reconnect,status}','spot_reconnect_reason',f #> '{spot_reconnect,reason}');
    FOREACH stage IN ARRAY ARRAY['intent','attempt','ack'] LOOP
        FOREACH clock_name IN ARRAY ARRAY['wall_ns','monotonic_ns'] LOOP
            d:=d||jsonb_build_object(stage||'_'||clock_name,(pub->>(stage||'_'||clock_name))::bigint);
        END LOOP;
    END LOOP;
    FOR fc IN SELECT value FROM jsonb_array_elements(f->'forecasts') LOOP
        h:=fc->>'horizon_s'; target:=s->'targets'->h;
        forecasts:=forecasts||jsonb_build_array(jsonb_build_object(
          'horizon_s',fc->'horizon_s','target_source_timestamp_ms',fc->'target_source_timestamp_ms',
          'forecast_price',(fc->>'price')::numeric(38,18)::text,'quality',fc->'quality','counts',fc->'counts',
          'max_interior_carry_ms',fc->'max_interior_carry_ms','reasons',fc->'reasons',
          'attempted_eligible',attempted AND coalesce((selection->'eligible_horizons') @> jsonb_build_array(h::integer),false),
          'exclusion_reasons',coalesce(selection->'excluded_horizons'->h,'[]'::jsonb),
          'estimated_arrival_wall_ns',(fc->>'estimated_arrival_wall_ns')::bigint,
          'estimated_remaining_ns',(fc->>'estimated_remaining_ns')::bigint,'target_status',target->'status',
          'first_event',public.ghost_twap_compact_event(target->'first_event'),
          'first_late_event',public.ghost_twap_compact_event(target->'first_late_event'),
          'first_conflicting_event',public.ghost_twap_compact_event(target->'first_conflicting_event'),
          'conflicted',target->'conflicted','clock_anomaly',coalesce((target->>'clock_anomaly')::boolean,false),
          'late_missing',coalesce((target->>'late_missing')::boolean,false),
          'error',(target->>'error')::numeric(38,18)::text,'persistence_error',(target->>'persistence_error')::numeric(38,18)::text,
          'eta_error_ns',(target->>'eta_error_ns')::bigint,'confirmed_redis_lead_ns',(target->>'confirmed_redis_lead_ns')::bigint));
    END LOOP;
    RETURN jsonb_build_object('schema_version',1,'decision',d,'horizons',forecasts);
END;
$$;
CREATE OR REPLACE FUNCTION ghost_twap_compact_commit(p_run TEXT,p_decision TEXT,p_version BIGINT,
    p_frozen TEXT,p_state TEXT,p_body TEXT,p_summary TEXT,p_previous_summary TEXT) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE a public.ghost_twap_audit%ROWTYPE; b JSONB; s JSONB; h BIGINT; prior TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(p_run||chr(31)||p_decision,917));
    SELECT * INTO a FROM public.ghost_twap_audit WHERE run_id=p_run AND decision_id=p_decision FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;
    IF NOT a.terminal OR a.version<>p_version OR a.frozen_sha256<>p_frozen OR a.state_sha256<>p_state
       OR (a.frozen_json::jsonb #> '{runtime_policy,continuous}') IS DISTINCT FROM 'true'::jsonb
       OR a.decision_wall_ns>(extract(epoch FROM clock_timestamp())*1000000000)::bigint-120000000000 THEN
        RETURN false;
    END IF;
    b:=p_body::jsonb; s:=p_summary::jsonb; h:=(a.created_ms/3600000)*3600000;
    IF b-'compact_sha256' IS DISTINCT FROM public.ghost_twap_compact_expected(
         a.frozen_json::jsonb,a.state_json::jsonb,a.version,a.created_ms,a.frozen_sha256,a.state_sha256) THEN
        RAISE EXCEPTION 'compact evidence does not match frozen calculation and original outcomes';
    END IF;
    IF octet_length(p_body)>131072 OR octet_length(p_summary)>2097152
       OR b #>> '{decision,run_id}' IS DISTINCT FROM p_run OR b #>> '{decision,decision_id}' IS DISTINCT FROM p_decision
       OR (b #>> '{decision,record_version}')::bigint IS DISTINCT FROM p_version
       OR b #>> '{decision,frozen_sha256}' IS DISTINCT FROM p_frozen OR b #>> '{decision,state_sha256}' IS DISTINCT FROM p_state
       OR (b #>> '{decision,decision_wall_ns}')::bigint IS DISTINCT FROM a.decision_wall_ns
       OR (b #>> '{decision,created_ms}')::bigint IS DISTINCT FROM a.created_ms
       OR coalesce((b #>> '{decision,compact_revision}')::bigint,0)<>0
       OR b->>'schema_version' IS DISTINCT FROM '1' OR jsonb_typeof(b->'horizons') IS DISTINCT FROM 'array'
       OR jsonb_array_length(b->'horizons') IS DISTINCT FROM 6
       OR (b->>'compact_sha256' ~ '^[0-9a-f]{64}$') IS DISTINCT FROM true
       OR (s->>'hour_start_ms')::bigint IS DISTINCT FROM h OR s->>'schema_version' IS DISTINCT FROM '1'
       OR jsonb_typeof(s->'groups') IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'invalid compact/summary lineage';
    END IF;
    PERFORM pg_advisory_xact_lock(-h-918);
    SELECT body_sha256 INTO prior FROM public.ghost_twap_accuracy_hourly WHERE hour_start_ms=h FOR UPDATE;
    IF prior IS DISTINCT FROM p_previous_summary THEN RETURN false; END IF;
    INSERT INTO public.ghost_twap_accuracy_hourly(hour_start_ms,body_json,body_sha256)
        VALUES(h,p_summary,encode(sha256(convert_to(p_summary,'UTF8')),'hex'))
        ON CONFLICT(hour_start_ms) DO UPDATE SET body_json=excluded.body_json,body_sha256=excluded.body_sha256,updated_at=clock_timestamp();
    INSERT INTO public.ghost_twap_compact(run_id,decision_id,decision_wall_ns,created_ms,version,frozen_sha256,state_sha256,
        target_source_timestamps_ms,hour_start_ms,body_json,body_sha256,compact_revision,summary_revision)
        VALUES(a.run_id,a.decision_id,a.decision_wall_ns,a.created_ms,a.version,a.frozen_sha256,a.state_sha256,
          a.target_source_timestamps_ms,h,p_body,encode(sha256(convert_to(p_body,'UTF8')),'hex'),0,0);
    DELETE FROM public.ghost_twap_audit WHERE run_id=a.run_id AND decision_id=a.decision_id;
    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION ghost_twap_compact_annotate(p_run TEXT,p_decision TEXT,p_previous_body TEXT,
    p_body TEXT,p_summary TEXT,p_previous_summary TEXT) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE a public.ghost_twap_compact%ROWTYPE; b JSONB; s JSONB; prior TEXT; revision BIGINT;
    i INTEGER; old_h JSONB; new_h JSONB; field TEXT; event JSONB;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(p_run||chr(31)||p_decision,917));
    SELECT * INTO a FROM public.ghost_twap_compact WHERE run_id=p_run AND decision_id=p_decision FOR UPDATE;
    IF NOT FOUND OR a.body_sha256 IS DISTINCT FROM p_previous_body THEN RETURN false; END IF;
    b:=p_body::jsonb; s:=p_summary::jsonb; revision:=(b #>> '{decision,compact_revision}')::bigint;
    IF octet_length(p_body)>131072 OR octet_length(p_summary)>2097152
       OR (b->'decision')-'compact_revision' IS DISTINCT FROM (a.body_json::jsonb->'decision')-'compact_revision'
       OR revision IS DISTINCT FROM a.compact_revision+1
       OR b->>'schema_version' IS DISTINCT FROM '1' OR jsonb_typeof(b->'horizons') IS DISTINCT FROM 'array'
       OR jsonb_array_length(b->'horizons') IS DISTINCT FROM 6
       OR (b->>'compact_sha256' ~ '^[0-9a-f]{64}$') IS DISTINCT FROM true
       OR (s->>'hour_start_ms')::bigint IS DISTINCT FROM a.hour_start_ms
       OR s->>'schema_version' IS DISTINCT FROM '1' OR jsonb_typeof(s->'groups') IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'invalid compact annotation lineage';
    END IF;
    FOR i IN 0..5 LOOP
        old_h:=a.body_json::jsonb->'horizons'->i; new_h:=b->'horizons'->i;
        IF old_h-ARRAY['first_late_event','first_conflicting_event','conflicted','late_missing','confirmed_redis_lead_ns']
           IS DISTINCT FROM new_h-ARRAY['first_late_event','first_conflicting_event','conflicted','late_missing','confirmed_redis_lead_ns'] THEN
            RAISE EXCEPTION 'compact first outcome/calculation is immutable';
        END IF;
        FOREACH field IN ARRAY ARRAY['first_late_event','first_conflicting_event'] LOOP
            IF old_h->field IS DISTINCT FROM 'null'::jsonb AND old_h->field IS DISTINCT FROM new_h->field THEN
                RAISE EXCEPTION 'compact first late/conflict event is immutable';
            END IF;
            IF old_h->field='null'::jsonb AND new_h->field IS DISTINCT FROM 'null'::jsonb THEN
                event:=new_h->field;
                IF jsonb_typeof(event) IS DISTINCT FROM 'object'
                   OR event->'source_timestamp_ms' IS DISTINCT FROM new_h->'target_source_timestamp_ms'
                   OR event->>'window_s' IS DISTINCT FROM '60'
                   OR (event->>'source_timestamp_ms')::bigint*1000000>(event->>'received_wall_ns')::bigint
                   OR NOT ((event->>'value')::numeric>0)
                   OR (field='first_late_event' AND (new_h->>'target_status' NOT IN ('missing','restart_unmatched')
                       OR new_h->'late_missing' IS DISTINCT FROM 'true'::jsonb))
                   OR (field='first_conflicting_event' AND (new_h->>'target_status' IS DISTINCT FROM 'matched'
                       OR new_h->'conflicted' IS DISTINCT FROM 'true'::jsonb
                       OR (event->>'value')::numeric=(new_h #>> '{first_event,value}')::numeric)) THEN
                    RAISE EXCEPTION 'invalid compact late/conflict annotation';
                END IF;
            END IF;
        END LOOP;
        FOREACH field IN ARRAY ARRAY['conflicted','late_missing'] LOOP
            IF jsonb_typeof(new_h->field) IS DISTINCT FROM 'boolean'
               OR (old_h->field='true'::jsonb AND new_h->field IS DISTINCT FROM 'true'::jsonb) THEN
                RAISE EXCEPTION 'compact late/conflict flag cannot clear';
            END IF;
        END LOOP;
        IF (new_h->'conflicted'='true'::jsonb AND old_h->'conflicted'='false'::jsonb
               AND (new_h->'first_conflicting_event' IS NULL OR new_h->'first_conflicting_event'='null'::jsonb))
           OR (new_h->'late_missing'='true'::jsonb AND old_h->'late_missing'='false'::jsonb
               AND (new_h->'first_late_event' IS NULL OR new_h->'first_late_event'='null'::jsonb)) THEN
            RAISE EXCEPTION 'compact flags require corresponding observed evidence';
        END IF;
        IF old_h->'confirmed_redis_lead_ns' IS DISTINCT FROM new_h->'confirmed_redis_lead_ns'
           AND NOT (new_h->'confirmed_redis_lead_ns'='null'::jsonb AND new_h->'conflicted'='true'::jsonb) THEN
            RAISE EXCEPTION 'compact confirmed lead cannot be invented';
        END IF;
    END LOOP;
    PERFORM pg_advisory_xact_lock(-a.hour_start_ms-918);
    SELECT body_sha256 INTO prior FROM public.ghost_twap_accuracy_hourly WHERE hour_start_ms=a.hour_start_ms FOR UPDATE;
    IF prior IS NULL OR prior IS DISTINCT FROM p_previous_summary THEN RETURN false; END IF;
    UPDATE public.ghost_twap_accuracy_hourly SET body_json=p_summary,body_sha256=encode(sha256(convert_to(p_summary,'UTF8')),'hex'),updated_at=clock_timestamp()
        WHERE hour_start_ms=a.hour_start_ms;
    UPDATE public.ghost_twap_compact SET body_json=p_body,body_sha256=encode(sha256(convert_to(p_body,'UTF8')),'hex'),
        compact_revision=revision,summary_revision=revision,updated_at=clock_timestamp() WHERE run_id=p_run AND decision_id=p_decision;
    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION ghost_twap_retention_expire(p_now_ms BIGINT,p_limit INTEGER)
RETURNS TABLE(expired BIGINT,summary_expired BIGINT,feed_expired BIGINT)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE n BIGINT;
BEGIN
    IF p_limit IS NULL OR p_limit<1 OR p_limit>100 OR p_now_ms IS NULL OR p_now_ms<0 THEN
        RAISE EXCEPTION 'invalid retention bound';
    END IF;
    n:=least(p_now_ms,(extract(epoch FROM clock_timestamp())*1000)::bigint);
    WITH candidates AS (SELECT run_id,decision_id FROM public.ghost_twap_compact WHERE created_ms<=n-604800000
        ORDER BY created_ms,run_id,decision_id LIMIT p_limit FOR UPDATE SKIP LOCKED)
    DELETE FROM public.ghost_twap_compact a USING candidates c WHERE a.run_id=c.run_id AND a.decision_id=c.decision_id;
    GET DIAGNOSTICS expired=ROW_COUNT;
    WITH candidates AS (SELECT hour_start_ms FROM public.ghost_twap_accuracy_hourly h WHERE hour_start_ms+3600000<=n-7776000000
        AND NOT EXISTS(SELECT 1 FROM public.ghost_twap_compact c WHERE c.hour_start_ms=h.hour_start_ms)
        ORDER BY hour_start_ms LIMIT p_limit FOR UPDATE SKIP LOCKED)
    DELETE FROM public.ghost_twap_accuracy_hourly h USING candidates c WHERE h.hour_start_ms=c.hour_start_ms;
    GET DIAGNOSTICS summary_expired=ROW_COUNT;
    WITH candidates AS (SELECT run_id,hour_start_ms FROM public.ghost_twap_feed_health WHERE hour_start_ms+3600000<=n-7776000000
        ORDER BY hour_start_ms,run_id LIMIT p_limit FOR UPDATE SKIP LOCKED)
    DELETE FROM public.ghost_twap_feed_health h USING candidates c WHERE h.run_id=c.run_id AND h.hour_start_ms=c.hour_start_ms;
    GET DIAGNOSTICS feed_expired=ROW_COUNT;
    UPDATE public.ghost_twap_retention_state SET expired_before_ms=greatest(expired_before_ms,n-604800000) WHERE singleton;
    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION ghost_twap_retention_expired_recovery() RETURNS VOID
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
    UPDATE public.ghost_twap_retention_state SET expired_recoveries=expired_recoveries+1 WHERE singleton;
$$;
CREATE OR REPLACE FUNCTION ghost_twap_retention_metadata(p_kind TEXT,p_body TEXT) RETURNS TEXT
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE previous JSONB; incoming JSONB; pair RECORD; result TEXT;
BEGIN
    incoming:=p_body::jsonb;
    IF octet_length(p_body)>2097152 OR incoming->>'schema_version' IS DISTINCT FROM '1'
       OR jsonb_typeof(incoming->'groups') IS DISTINCT FROM 'object' OR p_kind NOT IN ('baseline','warning') THEN
        RAISE EXCEPTION 'invalid retention metadata';
    END IF;
    SELECT CASE WHEN p_kind='baseline' THEN baseline_json ELSE warning_json END::jsonb INTO previous
        FROM public.ghost_twap_retention_state WHERE singleton FOR UPDATE;
    previous:=coalesce(previous,'{"schema_version":1,"groups":{}}'::jsonb);
    FOR pair IN SELECT key,value FROM jsonb_each(incoming->'groups') LOOP
        IF NOT (previous->'groups' ? pair.key) OR (p_kind='warning' AND
           (pair.value->>'last_evaluated_end_ms')::bigint>(previous #>> ARRAY['groups',pair.key,'last_evaluated_end_ms'])::bigint) THEN
            previous:=jsonb_set(previous,ARRAY['groups',pair.key],pair.value,true);
        END IF;
    END LOOP;
    result:=previous::text;
    IF octet_length(result)>2097152 THEN RAISE EXCEPTION 'retention metadata capacity exceeded'; END IF;
    IF p_kind='baseline' THEN UPDATE public.ghost_twap_retention_state SET baseline_json=result WHERE singleton;
    ELSE UPDATE public.ghost_twap_retention_state SET warning_json=result WHERE singleton; END IF;
    RETURN result;
END;
$$;
CREATE OR REPLACE FUNCTION ghost_twap_feed_health_commit(p_run TEXT,p_hour BIGINT,p_revision BIGINT,p_body TEXT) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE b JSONB; old_row public.ghost_twap_feed_health%ROWTYPE;
BEGIN
    b:=p_body::jsonb;
    IF length(p_run) NOT BETWEEN 1 AND 128 OR p_hour<0 OR p_hour%3600000<>0 OR p_revision<0
       OR octet_length(p_body)>131072 OR b->>'run_id' IS DISTINCT FROM p_run
       OR (b->>'hour_start_ms')::bigint IS DISTINCT FROM p_hour OR (b->>'revision')::bigint IS DISTINCT FROM p_revision
       OR jsonb_typeof(b->'complete') IS DISTINCT FROM 'boolean' OR jsonb_typeof(b->'feeds') IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'invalid feed health snapshot';
    END IF;
    IF p_hour+3600000<=(extract(epoch FROM clock_timestamp())*1000)::bigint-7776000000 THEN RETURN; END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(p_run||chr(31)||p_hour::text,919));
    SELECT * INTO old_row FROM public.ghost_twap_feed_health WHERE run_id=p_run AND hour_start_ms=p_hour FOR UPDATE;
    IF FOUND THEN
        IF p_revision<old_row.revision THEN RETURN; END IF;
        IF p_revision=old_row.revision THEN
            IF p_body::jsonb IS DISTINCT FROM old_row.body_json::jsonb THEN RAISE EXCEPTION 'same feed health revision changed'; END IF;
            RETURN;
        END IF;
        IF old_row.body_json::jsonb->'complete'='true'::jsonb AND b->'complete'<>'true'::jsonb THEN
            RAISE EXCEPTION 'complete feed health cannot regress';
        END IF;
    END IF;
    INSERT INTO public.ghost_twap_feed_health(run_id,hour_start_ms,revision,body_json,body_sha256)
        VALUES(p_run,p_hour,p_revision,p_body,encode(sha256(convert_to(p_body,'UTF8')),'hex'))
        ON CONFLICT(run_id,hour_start_ms) DO UPDATE SET revision=excluded.revision,body_json=excluded.body_json,body_sha256=excluded.body_sha256;
END;
$$;

-- Functions run as the migration owner with a locked path. No collector may
-- mutate summaries/attestations or delete rows outside these bounded functions.
ALTER TABLE public.ghost_twap_compact OWNER TO CURRENT_USER;
ALTER TABLE public.ghost_twap_accuracy_hourly OWNER TO CURRENT_USER;
ALTER TABLE public.ghost_twap_retention_state OWNER TO CURRENT_USER;
ALTER TABLE public.ghost_twap_feed_health OWNER TO CURRENT_USER;
REVOKE ALL ON public.ghost_twap_compact,public.ghost_twap_accuracy_hourly,
    public.ghost_twap_retention_state,public.ghost_twap_feed_health FROM PUBLIC,price_writer,price_reader;
GRANT SELECT ON public.ghost_twap_compact,public.ghost_twap_accuracy_hourly,
    public.ghost_twap_retention_state,public.ghost_twap_feed_health TO price_writer,price_reader;
REVOKE ALL ON FUNCTION ghost_twap_audit_guard(),ghost_twap_retention_count(),
    ghost_twap_retention_insert_guard(),ghost_twap_compact_delete_guard(),
    ghost_twap_compact_event(JSONB),ghost_twap_compact_expected(JSONB,JSONB,BIGINT,BIGINT,TEXT,TEXT),
    ghost_twap_compact_commit(TEXT,TEXT,BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT),
    ghost_twap_compact_annotate(TEXT,TEXT,TEXT,TEXT,TEXT,TEXT),
    ghost_twap_retention_expire(BIGINT,INTEGER),ghost_twap_retention_expired_recovery(),
    ghost_twap_retention_metadata(TEXT,TEXT),ghost_twap_feed_health_commit(TEXT,BIGINT,BIGINT,TEXT)
    FROM PUBLIC,price_reader,price_writer;
GRANT EXECUTE ON FUNCTION ghost_twap_compact_commit(TEXT,TEXT,BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT),
    ghost_twap_compact_annotate(TEXT,TEXT,TEXT,TEXT,TEXT,TEXT),
    ghost_twap_retention_expire(BIGINT,INTEGER),ghost_twap_retention_expired_recovery(),
    ghost_twap_retention_metadata(TEXT,TEXT),ghost_twap_feed_health_commit(TEXT,BIGINT,BIGINT,TEXT) TO price_writer;
CREATE TABLE IF NOT EXISTS providers (
    provider_id SMALLSERIAL PRIMARY KEY,
    provider_code TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS instruments (
    instrument_id BIGSERIAL PRIMARY KEY,
    provider_id SMALLINT NOT NULL REFERENCES providers(provider_id),
    symbol TEXT NOT NULL,
    base_asset TEXT NOT NULL,
    quote_asset TEXT NOT NULL,
    stream_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider_id, symbol)
);

CREATE TABLE IF NOT EXISTS market_windows (
    market_id BIGINT PRIMARY KEY,
    market_start_ms BIGINT NOT NULL UNIQUE,
    market_end_ms BIGINT NOT NULL,
    market_start_at TIMESTAMPTZ NOT NULL,
    market_end_at TIMESTAMPTZ NOT NULL,

    CHECK (market_start_ms % 300000 = 0),
    CHECK (market_end_ms = market_start_ms + 300000),
    CHECK (market_id = market_start_ms / 300000)
);

CREATE TABLE IF NOT EXISTS polymarket_btc_5m_markets (
    market_id BIGINT PRIMARY KEY REFERENCES market_windows(market_id),

    slug TEXT NOT NULL UNIQUE,
    gamma_event_id TEXT,
    gamma_market_id TEXT,
    condition_id TEXT,

    question TEXT,
    start_ms BIGINT,
    end_ms BIGINT,
    start_at TIMESTAMPTZ,
    end_at TIMESTAMPTZ,

    up_token_id TEXT NOT NULL,
    down_token_id TEXT NOT NULL,
    up_outcome TEXT NOT NULL DEFAULT 'Up',
    down_outcome TEXT NOT NULL DEFAULT 'Down',

    active BOOLEAN,
    closed BOOLEAN,
    archived BOOLEAN,

    settlement_reference TEXT NOT NULL DEFAULT 'unknown',
    settlement_window_s SMALLINT,
    settlement_source_url TEXT,
    settlement_rule_version TEXT,

    raw_gamma JSONB,
    first_seen_ms BIGINT NOT NULL,
    last_seen_ms BIGINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CHECK (market_id >= 0),
    CONSTRAINT polymarket_btc_5m_markets_settlement_reference_check CHECK (
        settlement_reference IN (
            'chainlink_spot',
            'chainlink_twap',
            'unknown'
        )
    ),
    CONSTRAINT polymarket_btc_5m_markets_settlement_window_check CHECK (
        settlement_window_s IS NULL OR settlement_window_s > 0
    ),
    CONSTRAINT polymarket_btc_5m_markets_settlement_complete_check CHECK (
        settlement_reference <> 'chainlink_twap'
        OR (
            settlement_window_s IS NOT NULL
            AND settlement_source_url IS NOT NULL
            AND settlement_rule_version IS NOT NULL
        )
    )
);

ALTER TABLE polymarket_btc_5m_markets
    ADD COLUMN IF NOT EXISTS settlement_reference TEXT
        NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS settlement_window_s SMALLINT,
    ADD COLUMN IF NOT EXISTS settlement_source_url TEXT,
    ADD COLUMN IF NOT EXISTS settlement_rule_version TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'polymarket_btc_5m_markets'::regclass
          AND conname =
                'polymarket_btc_5m_markets_settlement_reference_check'
    ) THEN
        ALTER TABLE polymarket_btc_5m_markets
            ADD CONSTRAINT
                polymarket_btc_5m_markets_settlement_reference_check
            CHECK (
                settlement_reference IN (
                    'chainlink_spot',
                    'chainlink_twap',
                    'unknown'
                )
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'polymarket_btc_5m_markets'::regclass
          AND conname =
                'polymarket_btc_5m_markets_settlement_window_check'
    ) THEN
        ALTER TABLE polymarket_btc_5m_markets
            ADD CONSTRAINT
                polymarket_btc_5m_markets_settlement_window_check
            CHECK (settlement_window_s IS NULL OR settlement_window_s > 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'polymarket_btc_5m_markets'::regclass
          AND conname =
                'polymarket_btc_5m_markets_settlement_complete_check'
    ) THEN
        ALTER TABLE polymarket_btc_5m_markets
            ADD CONSTRAINT
                polymarket_btc_5m_markets_settlement_complete_check
            CHECK (
                settlement_reference <> 'chainlink_twap'
                OR (
                    settlement_window_s IS NOT NULL
                    AND settlement_source_url IS NOT NULL
                    AND settlement_rule_version IS NOT NULL
                )
            );
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS polymarket_btc_5m_markets_slug_idx
    ON polymarket_btc_5m_markets (slug);

CREATE INDEX IF NOT EXISTS polymarket_btc_5m_markets_tokens_idx
    ON polymarket_btc_5m_markets (up_token_id, down_token_id);

CREATE TABLE IF NOT EXISTS polymarket_btc_5m_resolutions (
    market_id BIGINT PRIMARY KEY REFERENCES polymarket_btc_5m_markets(market_id),

    resolution_status TEXT NOT NULL DEFAULT 'pending',
    resolution_type TEXT,

    chainlink_open_price NUMERIC(38, 18),
    chainlink_close_price NUMERIC(38, 18),
    chainlink_source TEXT,

    winner TEXT,
    winning_token_id TEXT,
    up_payout NUMERIC(18, 8),
    down_payout NUMERIC(18, 8),

    resolved_at_ms BIGINT,
    resolution_source TEXT,
    raw_resolution JSONB,
    reconciled_settlement_rule_version TEXT,

    first_checked_ms BIGINT NOT NULL,
    last_checked_ms BIGINT NOT NULL,
    next_check_ms BIGINT,
    resolution_attempts INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CHECK (resolution_status IN ('pending', 'resolved')),
    CHECK (resolution_type IS NULL OR resolution_type IN ('winner', 'split')),
    CHECK (chainlink_open_price IS NULL OR chainlink_open_price > 0),
    CHECK (chainlink_close_price IS NULL OR chainlink_close_price > 0),
    CHECK (
        (chainlink_open_price IS NULL AND chainlink_close_price IS NULL
            AND chainlink_source IS NULL)
        OR
        ((chainlink_open_price IS NOT NULL OR chainlink_close_price IS NOT NULL)
            AND chainlink_source IS NOT NULL)
    ),
    CHECK (up_payout IS NULL OR (up_payout >= 0 AND up_payout <= 1)),
    CHECK (down_payout IS NULL OR (down_payout >= 0 AND down_payout <= 1)),
    CHECK (resolved_at_ms IS NULL OR resolved_at_ms >= 0),
    CHECK (last_checked_ms >= first_checked_ms),
    CHECK (next_check_ms IS NULL OR next_check_ms >= last_checked_ms),
    CHECK (resolution_attempts >= 0),
    CHECK (
        (
            resolution_status = 'pending'
            AND resolution_type IS NULL
            AND winner IS NULL
            AND winning_token_id IS NULL
            AND up_payout IS NULL
            AND down_payout IS NULL
            AND resolved_at_ms IS NULL
            AND resolution_source IS NULL
        )
        OR
        (
            resolution_status = 'resolved'
            AND resolution_type IS NOT NULL
            AND resolution_type = 'winner'
            AND resolution_source IS NOT NULL
            AND winner IS NOT NULL
            AND winning_token_id IS NOT NULL
            AND up_payout IS NOT NULL
            AND down_payout IS NOT NULL
            AND (
                (winner = 'Up' AND up_payout = 1 AND down_payout = 0)
                OR
                (winner = 'Down' AND up_payout = 0 AND down_payout = 1)
            )
        )
        OR
        (
            resolution_status = 'resolved'
            AND resolution_type IS NOT NULL
            AND resolution_type = 'split'
            AND winner IS NULL
            AND winning_token_id IS NULL
            AND up_payout IS NOT NULL
            AND down_payout IS NOT NULL
            AND up_payout = 0.5
            AND down_payout = 0.5
            AND resolution_source IS NOT NULL
        )
    )
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_attribute
        WHERE attrelid = 'polymarket_btc_5m_resolutions'::regclass
          AND attname = 'reconciled_settlement_rule_version'
          AND NOT attisdropped
    ) THEN
        ALTER TABLE polymarket_btc_5m_resolutions
            ADD COLUMN IF NOT EXISTS reconciled_settlement_rule_version TEXT;

        -- Stamp pre-migration complete rows only when their stored market
        -- metadata is an exact rule/time match.  This is deliberately inside
        -- the one-time column migration: reapplying schema.sql must never
        -- bless a stale resolution after market metadata has been repaired.
        UPDATE polymarket_btc_5m_resolutions AS resolution
        SET reconciled_settlement_rule_version = market.settlement_rule_version
        FROM polymarket_btc_5m_markets AS market
        JOIN market_windows AS mw ON mw.market_id = market.market_id
        WHERE resolution.market_id = market.market_id
          AND resolution.resolution_status = 'resolved'
          AND resolution.resolution_type IS NOT NULL
          AND resolution.chainlink_open_price IS NOT NULL
          AND resolution.chainlink_close_price IS NOT NULL
          AND (
                (
                    market.settlement_reference = 'chainlink_spot'
                    AND market.settlement_rule_version = 'chainlink-spot-v1'
                )
                OR (
                    mw.market_start_ms < 1786665600000
                    AND market.settlement_reference = 'chainlink_twap'
                    AND market.settlement_window_s = 30
                    AND market.settlement_source_url =
                        'https://data.chain.link/streams/btc-usd-twap-30s-streams'
                    AND market.settlement_rule_version = 'btc-5m-twap-30'
                )
                OR (
                    mw.market_start_ms >= 1786665600000
                    AND market.settlement_reference = 'chainlink_twap'
                    AND market.settlement_window_s = 60
                    AND market.settlement_source_url =
                        'https://data.chain.link/streams/btc-usd-twap-60s-streams'
                    AND market.settlement_rule_version = 'btc-5m-twap-60'
                )
          );
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS polymarket_btc_5m_resolutions_due_idx
    ON polymarket_btc_5m_resolutions (next_check_ms, market_id)
    WHERE next_check_ms IS NOT NULL;

CREATE TABLE IF NOT EXISTS price_samples (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    sample_second_ms BIGINT NOT NULL,
    sample_second_at TIMESTAMPTZ NOT NULL,

    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),

    price NUMERIC(38, 18) NOT NULL,
    provider_event_ms BIGINT,
    received_ms BIGINT NOT NULL,

    source_price_field TEXT NOT NULL DEFAULT 'c',
    provider_message_ms BIGINT,
    source_topic TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (instrument_id, sample_second_ms),

    CHECK (sample_second_ms % 1000 = 0),
    CHECK (price > 0),
    CHECK (sample_second_ms >= market_id * 300000),
    CHECK (sample_second_ms < (market_id + 1) * 300000)
);

ALTER TABLE price_samples
ADD COLUMN IF NOT EXISTS provider_message_ms BIGINT;

ALTER TABLE price_samples
ADD COLUMN IF NOT EXISTS source_topic TEXT;

CREATE INDEX IF NOT EXISTS price_samples_market_idx
    ON price_samples (market_id, instrument_id, sample_second_ms);

CREATE INDEX IF NOT EXISTS price_samples_instrument_latest_idx
    ON price_samples (instrument_id, sample_second_ms DESC);

-- The free RTDS TWAP relay has no snapshot, history, or replay. These tables
-- therefore preserve every accepted message and every connection gap instead
-- of relying on the one-second price_samples materialization alone.
CREATE TABLE IF NOT EXISTS polymarket_twap_sessions (
    connection_id UUID PRIMARY KEY,
    topic TEXT NOT NULL,
    symbol TEXT NOT NULL,
    window_s SMALLINT NOT NULL,

    connected_wall_ns BIGINT NOT NULL,
    connected_monotonic_ns BIGINT NOT NULL,
    subscribed_wall_ns BIGINT NOT NULL,
    subscribed_monotonic_ns BIGINT NOT NULL,

    disconnected_wall_ns BIGINT,
    disconnected_monotonic_ns BIGINT,
    close_reason TEXT,
    messages_received_total BIGINT NOT NULL DEFAULT 0,
    messages_accepted_total BIGINT NOT NULL DEFAULT 0,
    parse_errors_total BIGINT NOT NULL DEFAULT 0,
    last_receive_sequence BIGINT NOT NULL DEFAULT 0,
    last_accepted_received_ms BIGINT,
    last_provider_event_ms BIGINT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CHECK (window_s > 0),
    CHECK (connected_wall_ns > 0),
    CHECK (connected_monotonic_ns > 0),
    CHECK (subscribed_monotonic_ns >= connected_monotonic_ns),
    CHECK (
        (disconnected_wall_ns IS NULL)
        = (disconnected_monotonic_ns IS NULL)
    ),
    CHECK ((disconnected_wall_ns IS NULL) = (close_reason IS NULL)),
    CHECK (
        disconnected_monotonic_ns IS NULL
        OR disconnected_monotonic_ns >= subscribed_monotonic_ns
    ),
    CHECK (messages_received_total >= 0),
    CHECK (messages_accepted_total >= 0),
    CHECK (parse_errors_total >= 0),
    CHECK (last_receive_sequence >= 0),
    CHECK (messages_accepted_total <= messages_received_total),
    CHECK (parse_errors_total <= messages_received_total),
    CHECK (
        messages_accepted_total + parse_errors_total
        <= messages_received_total
    )
);

CREATE TABLE IF NOT EXISTS polymarket_twap_events (
    connection_id UUID NOT NULL
        REFERENCES polymarket_twap_sessions(connection_id),
    receive_sequence BIGINT NOT NULL,
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    topic TEXT NOT NULL,
    symbol TEXT NOT NULL,
    window_s SMALLINT NOT NULL,

    provider_event_ms BIGINT NOT NULL,
    provider_message_ms BIGINT,
    received_wall_ns BIGINT NOT NULL,
    received_monotonic_ns BIGINT NOT NULL,

    sample_second_ms BIGINT NOT NULL,
    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),
    price_e18 NUMERIC(78, 0) NOT NULL,
    price NUMERIC(38, 18) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (connection_id, receive_sequence),

    CHECK (receive_sequence >= 1),
    CHECK (window_s > 0),
    CHECK (provider_event_ms >= 0),
    CHECK (provider_message_ms IS NULL OR provider_message_ms >= 0),
    CHECK (received_wall_ns > 0),
    CHECK (received_monotonic_ns > 0),
    CHECK (sample_second_ms % 1000 = 0),
    CHECK (sample_second_ms = (provider_event_ms / 1000) * 1000),
    CHECK (sample_second_ms >= market_id * 300000),
    CHECK (sample_second_ms < (market_id + 1) * 300000),
    CHECK (price_e18 > 0),
    CHECK (price > 0),
    CHECK (price_e18 = price * 1000000000000000000)
);

CREATE INDEX IF NOT EXISTS polymarket_twap_events_source_time_idx
    ON polymarket_twap_events (
        symbol,
        window_s,
        provider_event_ms,
        received_wall_ns
    );

CREATE INDEX IF NOT EXISTS polymarket_twap_events_market_idx
    ON polymarket_twap_events (market_id, provider_event_ms);

CREATE TABLE IF NOT EXISTS polymarket_twap_gaps (
    connection_id UUID NOT NULL
        REFERENCES polymarket_twap_sessions(connection_id),
    detected_wall_ns BIGINT NOT NULL,
    detected_monotonic_ns BIGINT NOT NULL,
    reason TEXT NOT NULL,
    idle_timeout_ms BIGINT,
    last_accepted_received_ms BIGINT,
    last_provider_event_ms BIGINT,
    messages_received_total BIGINT NOT NULL,
    messages_accepted_total BIGINT NOT NULL,
    parse_errors_total BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (connection_id, detected_wall_ns, reason),

    CHECK (detected_wall_ns > 0),
    CHECK (detected_monotonic_ns > 0),
    CHECK (idle_timeout_ms IS NULL OR idle_timeout_ms > 0),
    CHECK (messages_received_total >= 0),
    CHECK (messages_accepted_total >= 0),
    CHECK (parse_errors_total >= 0),
    CHECK (messages_accepted_total <= messages_received_total),
    CHECK (parse_errors_total <= messages_received_total),
    CHECK (
        messages_accepted_total + parse_errors_total
        <= messages_received_total
    )
);

CREATE INDEX IF NOT EXISTS polymarket_twap_gaps_detected_idx
    ON polymarket_twap_gaps (detected_wall_ns DESC);


REVOKE ALL ON polymarket_twap_sessions FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON polymarket_twap_sessions TO price_writer;
GRANT SELECT ON polymarket_twap_sessions TO price_reader;

REVOKE ALL ON polymarket_twap_events FROM PUBLIC;
GRANT SELECT, INSERT ON polymarket_twap_events TO price_writer;
GRANT SELECT ON polymarket_twap_events TO price_reader;

REVOKE ALL ON polymarket_twap_gaps FROM PUBLIC;
GRANT SELECT, INSERT ON polymarket_twap_gaps TO price_writer;
GRANT SELECT ON polymarket_twap_gaps TO price_reader;

CREATE TABLE IF NOT EXISTS polymarket_probability_samples (
    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),
    source TEXT NOT NULL DEFAULT 'polymarket_clob',

    sample_second_ms BIGINT NOT NULL,
    sample_second_at TIMESTAMPTZ NOT NULL,

    up_token_id TEXT NOT NULL,
    down_token_id TEXT NOT NULL,

    up_bid NUMERIC(18, 8),
    up_ask NUMERIC(18, 8),
    up_mid NUMERIC(18, 8),

    down_bid NUMERIC(18, 8),
    down_ask NUMERIC(18, 8),
    down_mid NUMERIC(18, 8),

    up_prob_norm NUMERIC(18, 8),
    down_prob_norm NUMERIC(18, 8),

    up_provider_event_ms BIGINT,
    up_received_ms BIGINT,
    down_provider_event_ms BIGINT,
    down_received_ms BIGINT,

    provider_event_ms BIGINT,
    received_ms BIGINT NOT NULL,

    raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (market_id, source, sample_second_ms),

    CHECK (sample_second_ms % 1000 = 0),
    CHECK (sample_second_ms >= market_id * 300000),
    CHECK (sample_second_ms < (market_id + 1) * 300000),

    CHECK (up_bid IS NULL OR (up_bid >= 0 AND up_bid <= 1)),
    CHECK (up_ask IS NULL OR (up_ask >= 0 AND up_ask <= 1)),
    CHECK (up_mid IS NULL OR (up_mid >= 0 AND up_mid <= 1)),

    CHECK (down_bid IS NULL OR (down_bid >= 0 AND down_bid <= 1)),
    CHECK (down_ask IS NULL OR (down_ask >= 0 AND down_ask <= 1)),
    CHECK (down_mid IS NULL OR (down_mid >= 0 AND down_mid <= 1)),

    CHECK (up_prob_norm IS NULL OR (up_prob_norm >= 0 AND up_prob_norm <= 1)),
    CHECK (down_prob_norm IS NULL OR (down_prob_norm >= 0 AND down_prob_norm <= 1))
);

ALTER TABLE polymarket_probability_samples
    ADD COLUMN IF NOT EXISTS up_provider_event_ms BIGINT,
    ADD COLUMN IF NOT EXISTS up_received_ms BIGINT,
    ADD COLUMN IF NOT EXISTS down_provider_event_ms BIGINT,
    ADD COLUMN IF NOT EXISTS down_received_ms BIGINT;

CREATE INDEX IF NOT EXISTS polymarket_probability_samples_market_idx
    ON polymarket_probability_samples (market_id, sample_second_ms);

CREATE INDEX IF NOT EXISTS polymarket_probability_samples_latest_idx
    ON polymarket_probability_samples (sample_second_ms DESC);

-- Compact prospective study evidence. Operator-owned history maintenance
-- expires these after ten days independently of optional raw_capture.
CREATE TABLE IF NOT EXISTS polymarket_evidence_payloads (
    payload_hash TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(payload) = 'object')
);

CREATE TABLE IF NOT EXISTS polymarket_market_observations (
    record_id UUID PRIMARY KEY,
    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),
    kind TEXT NOT NULL,
    received_wall_ns BIGINT NOT NULL,
    received_monotonic_ns BIGINT NOT NULL,
    requested_wall_ns BIGINT,
    requested_monotonic_ns BIGINT,
    http_status SMALLINT,
    status TEXT NOT NULL,
    connection_id UUID,
    provider_event_ms BIGINT,
    response_sha256 TEXT,
    response_date TEXT,
    response_age_seconds BIGINT,
    payload_hash TEXT NOT NULL REFERENCES polymarket_evidence_payloads(payload_hash),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (market_id >= 0),
    CHECK (kind IN (
        'price_to_beat', 'gamma_market', 'clob_market', 'clob_order_rules', 'tick_size',
        'session_start', 'session_end', 'gap'
    )),
    CHECK (status IN ('ok', 'missing', 'invalid', 'http_error', 'transport_error')),
    CHECK (received_wall_ns > 0),
    CHECK (received_monotonic_ns > 0),
    CHECK ((requested_wall_ns IS NULL) = (requested_monotonic_ns IS NULL)),
    CHECK (requested_wall_ns IS NULL OR requested_wall_ns > 0),
    CHECK (requested_monotonic_ns IS NULL OR (
        requested_monotonic_ns > 0 AND requested_monotonic_ns <= received_monotonic_ns
    )),
    CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    CHECK (provider_event_ms IS NULL OR provider_event_ms >= 0),
    CHECK (response_sha256 IS NULL OR response_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (response_age_seconds IS NULL OR response_age_seconds >= 0),
    CHECK (kind NOT IN ('session_start', 'session_end') OR connection_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS polymarket_market_observations_market_time_idx
    ON polymarket_market_observations (market_id, received_wall_ns);

-- Used by bounded orphan cleanup and the FK check when expiring payloads.
CREATE INDEX IF NOT EXISTS polymarket_market_observations_payload_idx
    ON polymarket_market_observations (payload_hash);
CREATE INDEX IF NOT EXISTS polymarket_evidence_payloads_retention_idx
    ON polymarket_evidence_payloads (created_at);

CREATE INDEX IF NOT EXISTS polymarket_market_observations_connection_idx
    ON polymarket_market_observations (connection_id, received_wall_ns)
    WHERE connection_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS polymarket_market_observations_session_kind_idx
    ON polymarket_market_observations (connection_id, kind)
    WHERE kind IN ('session_start', 'session_end');

-- These are sampled states at their actual observation clocks, not raw quote
-- events or scheduled/backdated boundaries. Receipt/component clocks may be
-- older or absent. A missing initial state remains an explicit nullable row.
-- There is deliberately no quote/session FK: the independent metadata writer
-- must not serialize the quote writer behind a slow metadata insert.
CREATE TABLE IF NOT EXISTS polymarket_quote_observations (
    connection_id UUID NOT NULL,
    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),
    observed_wall_ns BIGINT NOT NULL,
    observed_monotonic_ns BIGINT NOT NULL,
    receive_sequence BIGINT NOT NULL,
    received_wall_ns BIGINT,
    received_monotonic_ns BIGINT,
    up_bid NUMERIC(38, 18),
    up_ask NUMERIC(38, 18),
    down_bid NUMERIC(38, 18),
    down_ask NUMERIC(38, 18),
    up_bid_provider_event_ms BIGINT,
    up_ask_provider_event_ms BIGINT,
    down_bid_provider_event_ms BIGINT,
    down_ask_provider_event_ms BIGINT,
    up_bid_received_ms BIGINT,
    up_ask_received_ms BIGINT,
    down_bid_received_ms BIGINT,
    down_ask_received_ms BIGINT,
    event_type TEXT,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (connection_id, observed_wall_ns),
    CHECK (market_id >= 0),
    CHECK (observed_wall_ns > 0),
    CHECK (observed_monotonic_ns > 0),
    CHECK (receive_sequence >= 0),
    CHECK ((received_wall_ns IS NULL) = (received_monotonic_ns IS NULL)),
    CHECK (received_wall_ns IS NULL OR received_wall_ns > 0),
    CHECK (received_monotonic_ns IS NULL OR (
        received_monotonic_ns > 0 AND received_monotonic_ns <= observed_monotonic_ns
    )),
    CHECK (up_bid IS NULL OR up_bid BETWEEN 0 AND 1),
    CHECK (up_ask IS NULL OR up_ask BETWEEN 0 AND 1),
    CHECK (down_bid IS NULL OR down_bid BETWEEN 0 AND 1),
    CHECK (down_ask IS NULL OR down_ask BETWEEN 0 AND 1),
    CHECK (up_bid_provider_event_ms IS NULL OR up_bid_provider_event_ms >= 0),
    CHECK (up_ask_provider_event_ms IS NULL OR up_ask_provider_event_ms >= 0),
    CHECK (down_bid_provider_event_ms IS NULL OR down_bid_provider_event_ms >= 0),
    CHECK (down_ask_provider_event_ms IS NULL OR down_ask_provider_event_ms >= 0),
    CHECK (up_bid_received_ms IS NULL OR up_bid_received_ms >= 0),
    CHECK (up_ask_received_ms IS NULL OR up_ask_received_ms >= 0),
    CHECK (down_bid_received_ms IS NULL OR down_bid_received_ms >= 0),
    CHECK (down_ask_received_ms IS NULL OR down_ask_received_ms >= 0)
);

CREATE INDEX IF NOT EXISTS polymarket_quote_observations_market_time_idx
    ON polymarket_quote_observations (market_id, observed_wall_ns);

REVOKE ALL ON polymarket_evidence_payloads FROM PUBLIC;
GRANT SELECT, INSERT ON polymarket_evidence_payloads TO price_writer;
GRANT SELECT ON polymarket_evidence_payloads TO price_reader;

REVOKE ALL ON polymarket_market_observations FROM PUBLIC;
GRANT SELECT, INSERT ON polymarket_market_observations TO price_writer;
GRANT SELECT ON polymarket_market_observations TO price_reader;

REVOKE ALL ON polymarket_quote_observations FROM PUBLIC;
GRANT SELECT, INSERT ON polymarket_quote_observations TO price_writer;
GRANT SELECT ON polymarket_quote_observations TO price_reader;

CREATE TABLE IF NOT EXISTS binance_futures_snapshots (
    symbol TEXT NOT NULL,

    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),
    sample_second_ms BIGINT NOT NULL,
    sample_second_at TIMESTAMPTZ NOT NULL,

    futures_last_price NUMERIC(38, 18),
    futures_last_price_time_ms BIGINT,

    mark_price NUMERIC(38, 18),
    index_price NUMERIC(38, 18),
    last_funding_rate NUMERIC(38, 18),
    next_funding_time_ms BIGINT,
    premium_index_time_ms BIGINT,

    open_interest NUMERIC(38, 18),
    open_interest_time_ms BIGINT,

    oi_notional_usdt NUMERIC(38, 18),
    premium_bps NUMERIC(20, 8),

    received_ms BIGINT NOT NULL,
    raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (symbol, sample_second_ms),

    CHECK (sample_second_ms % 1000 = 0),
    CHECK (sample_second_ms >= market_id * 300000),
    CHECK (sample_second_ms < (market_id + 1) * 300000),
    CHECK (open_interest IS NULL OR open_interest >= 0)
);

CREATE INDEX IF NOT EXISTS binance_futures_snapshots_market_idx
    ON binance_futures_snapshots (market_id, sample_second_ms);

CREATE INDEX IF NOT EXISTS binance_futures_snapshots_latest_idx
    ON binance_futures_snapshots (symbol, sample_second_ms DESC);

CREATE TABLE IF NOT EXISTS binance_flow_1s (
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,

    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),
    sample_second_ms BIGINT NOT NULL,
    sample_second_at TIMESTAMPTZ NOT NULL,

    buy_base NUMERIC(38, 18) NOT NULL DEFAULT 0,
    sell_base NUMERIC(38, 18) NOT NULL DEFAULT 0,
    buy_quote NUMERIC(38, 18) NOT NULL DEFAULT 0,
    sell_quote NUMERIC(38, 18) NOT NULL DEFAULT 0,

    delta_quote NUMERIC(38, 18) NOT NULL DEFAULT 0,
    total_quote NUMERIC(38, 18) NOT NULL DEFAULT 0,
    taker_imbalance NUMERIC(20, 8),

    cvd_quote NUMERIC(38, 18) NOT NULL DEFAULT 0,
    cvd_10s NUMERIC(38, 18) NOT NULL DEFAULT 0,
    cvd_30s NUMERIC(38, 18) NOT NULL DEFAULT 0,
    imbalance_10s NUMERIC(20, 8),
    imbalance_30s NUMERIC(20, 8),

    agg_trade_count INTEGER NOT NULL DEFAULT 0,
    trade_count INTEGER NOT NULL DEFAULT 0,
    max_trade_quote NUMERIC(38, 18),

    first_agg_trade_id BIGINT,
    last_agg_trade_id BIGINT,
    last_trade_time_ms BIGINT,
    last_event_time_ms BIGINT,
    received_ms BIGINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (venue, symbol, sample_second_ms),

    CHECK (sample_second_ms % 1000 = 0),
    CHECK (sample_second_ms >= market_id * 300000),
    CHECK (sample_second_ms < (market_id + 1) * 300000),
    CHECK (buy_base >= 0),
    CHECK (sell_base >= 0),
    CHECK (buy_quote >= 0),
    CHECK (sell_quote >= 0),
    CHECK (total_quote >= 0),
    CHECK (agg_trade_count >= 0),
    CHECK (trade_count >= 0),
    CHECK (taker_imbalance IS NULL OR (taker_imbalance >= -1 AND taker_imbalance <= 1)),
    CHECK (imbalance_10s IS NULL OR (imbalance_10s >= -1 AND imbalance_10s <= 1)),
    CHECK (imbalance_30s IS NULL OR (imbalance_30s >= -1 AND imbalance_30s <= 1))
);

CREATE INDEX IF NOT EXISTS binance_flow_1s_market_idx
    ON binance_flow_1s (market_id, venue, symbol, sample_second_ms);

CREATE INDEX IF NOT EXISTS binance_flow_1s_latest_idx
    ON binance_flow_1s (venue, symbol, sample_second_ms DESC);

CREATE TABLE IF NOT EXISTS binance_book_1s (
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,

    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),

    sample_second_ms BIGINT NOT NULL,
    sample_second_at TIMESTAMPTZ NOT NULL,

    bid NUMERIC(38, 18) NOT NULL,
    ask NUMERIC(38, 18) NOT NULL,
    bid_qty NUMERIC(38, 18) NOT NULL,
    ask_qty NUMERIC(38, 18) NOT NULL,

    mid NUMERIC(38, 18) NOT NULL,
    spread NUMERIC(38, 18) NOT NULL,
    spread_bps NUMERIC(20, 8) NOT NULL,
    book_imbalance NUMERIC(20, 8),
    microprice NUMERIC(38, 18),

    update_id BIGINT,
    event_time_ms BIGINT,
    transaction_time_ms BIGINT,
    received_ms BIGINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (venue, symbol, sample_second_ms),

    CHECK (sample_second_ms % 1000 = 0),
    CHECK (sample_second_ms >= market_id * 300000),
    CHECK (sample_second_ms < (market_id + 1) * 300000),
    CHECK (bid > 0),
    CHECK (ask > 0),
    CHECK (ask >= bid),
    CHECK (bid_qty >= 0),
    CHECK (ask_qty >= 0),
    CHECK (mid > 0),
    CHECK (spread >= 0),
    CHECK (spread_bps >= 0),
    CHECK (book_imbalance IS NULL OR (book_imbalance >= -1 AND book_imbalance <= 1))
);

CREATE INDEX IF NOT EXISTS binance_book_1s_market_idx
    ON binance_book_1s (market_id, venue, symbol, sample_second_ms);

CREATE INDEX IF NOT EXISTS binance_book_1s_latest_idx
    ON binance_book_1s (venue, symbol, sample_second_ms DESC);

-- Causal, receipt-time-aligned research summaries. Fast Binance messages are
-- aggregated in memory; raw spot trades, depth frames, and forced-order frames
-- are intentionally not retained here.
CREATE TABLE IF NOT EXISTS binance_microstructure_1s (
    symbol TEXT NOT NULL,

    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),
    sample_second_ms BIGINT NOT NULL,
    sample_second_at TIMESTAMPTZ NOT NULL,

    schema_version SMALLINT NOT NULL,
    sample_span_ms BIGINT,
    sample_jitter_ms BIGINT NOT NULL,
    collector_healthy BOOLEAN NOT NULL,

    spot_mid NUMERIC(38, 18),
    spot_bid NUMERIC(38, 18),
    spot_ask NUMERIC(38, 18),
    spot_spread_bps NUMERIC(20, 8),
    spot_weighted_mid_offset_bps NUMERIC(20, 8),
    spot_imbalance_1 NUMERIC(20, 8),
    spot_imbalance_5 NUMERIC(20, 8),
    spot_imbalance_10 NUMERIC(20, 8),
    spot_bid_depth_usdt_10 NUMERIC(38, 18),
    spot_ask_depth_usdt_10 NUMERIC(38, 18),
    spot_book_age_ms BIGINT,
    spot_book_lag_ms BIGINT,
    spot_snapshot_bbo_ofi_usdt NUMERIC(38, 18),
    spot_book_snapshot_count INTEGER NOT NULL DEFAULT 0,

    spot_buy_usdt NUMERIC(38, 18) NOT NULL DEFAULT 0,
    spot_sell_usdt NUMERIC(38, 18) NOT NULL DEFAULT 0,
    spot_trade_id_span INTEGER NOT NULL DEFAULT 0,
    spot_aggtrade_count INTEGER NOT NULL DEFAULT 0,
    spot_max_aggtrade_usdt NUMERIC(38, 18),
    spot_vwap NUMERIC(38, 18),
    spot_trade_high NUMERIC(38, 18),
    spot_trade_low NUMERIC(38, 18),
    spot_last_trade NUMERIC(38, 18),
    spot_trade_age_ms BIGINT,
    spot_trade_lag_mean_ms NUMERIC(20, 8),
    spot_trade_lag_max_ms BIGINT,

    fut_mid NUMERIC(38, 18),
    fut_bid NUMERIC(38, 18),
    fut_ask NUMERIC(38, 18),
    fut_spread_bps NUMERIC(20, 8),
    fut_weighted_mid_offset_bps NUMERIC(20, 8),
    fut_imbalance_1 NUMERIC(20, 8),
    fut_imbalance_5 NUMERIC(20, 8),
    fut_imbalance_10 NUMERIC(20, 8),
    fut_bid_depth_usdt_10 NUMERIC(38, 18),
    fut_ask_depth_usdt_10 NUMERIC(38, 18),
    fut_book_age_ms BIGINT,
    fut_book_lag_ms BIGINT,
    fut_snapshot_bbo_ofi_usdt NUMERIC(38, 18),
    fut_book_snapshot_count INTEGER NOT NULL DEFAULT 0,

    fut_buy_usdt NUMERIC(38, 18) NOT NULL DEFAULT 0,
    fut_sell_usdt NUMERIC(38, 18) NOT NULL DEFAULT 0,
    fut_rpi_buy_usdt NUMERIC(38, 18),
    fut_rpi_sell_usdt NUMERIC(38, 18),
    fut_trade_id_span INTEGER NOT NULL DEFAULT 0,
    fut_aggtrade_count INTEGER NOT NULL DEFAULT 0,
    fut_max_aggtrade_usdt NUMERIC(38, 18),
    fut_vwap NUMERIC(38, 18),
    fut_trade_high NUMERIC(38, 18),
    fut_trade_low NUMERIC(38, 18),
    fut_last_trade NUMERIC(38, 18),
    fut_trade_age_ms BIGINT,
    fut_trade_lag_mean_ms NUMERIC(20, 8),
    fut_trade_lag_max_ms BIGINT,

    perp_spot_basis_bps NUMERIC(20, 8),
    spot_fut_book_skew_ms BIGINT,

    mark_price NUMERIC(38, 18),
    index_price NUMERIC(38, 18),
    mark_index_basis_bps NUMERIC(20, 8),
    funding_rate NUMERIC(38, 18),
    seconds_to_funding BIGINT,
    mark_age_ms BIGINT,
    mark_lag_ms BIGINT,

    open_interest_btc NUMERIC(38, 18),
    open_interest_usdt NUMERIC(38, 18),
    oi_age_ms BIGINT,
    oi_exchange_age_ms BIGINT,
    oi_http_lag_ms BIGINT,

    long_liq_usdt NUMERIC(38, 18) NOT NULL DEFAULT 0,
    short_liq_usdt NUMERIC(38, 18) NOT NULL DEFAULT 0,
    liq_snapshot_count INTEGER NOT NULL DEFAULT 0,
    liq_lag_mean_ms NUMERIC(20, 8),
    connection_errors INTEGER NOT NULL DEFAULT 0,

    received_ms BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (symbol, sample_second_ms),

    CHECK (sample_second_ms % 1000 = 0),
    CHECK (sample_second_ms >= market_id * 300000),
    CHECK (sample_second_ms < (market_id + 1) * 300000),
    CHECK (sample_jitter_ms >= 0),
    CHECK (spot_bid IS NULL OR spot_ask IS NULL OR spot_ask >= spot_bid),
    CHECK (fut_bid IS NULL OR fut_ask IS NULL OR fut_ask >= fut_bid),
    CHECK (spot_imbalance_1 IS NULL OR spot_imbalance_1 BETWEEN -1 AND 1),
    CHECK (spot_imbalance_5 IS NULL OR spot_imbalance_5 BETWEEN -1 AND 1),
    CHECK (spot_imbalance_10 IS NULL OR spot_imbalance_10 BETWEEN -1 AND 1),
    CHECK (fut_imbalance_1 IS NULL OR fut_imbalance_1 BETWEEN -1 AND 1),
    CHECK (fut_imbalance_5 IS NULL OR fut_imbalance_5 BETWEEN -1 AND 1),
    CHECK (fut_imbalance_10 IS NULL OR fut_imbalance_10 BETWEEN -1 AND 1),
    CHECK (spot_buy_usdt >= 0),
    CHECK (spot_sell_usdt >= 0),
    CHECK (fut_buy_usdt >= 0),
    CHECK (fut_sell_usdt >= 0),
    CHECK (fut_rpi_buy_usdt IS NULL OR fut_rpi_buy_usdt >= 0),
    CHECK (fut_rpi_sell_usdt IS NULL OR fut_rpi_sell_usdt >= 0),
    CHECK (long_liq_usdt >= 0),
    CHECK (short_liq_usdt >= 0),
    CHECK (open_interest_btc IS NULL OR open_interest_btc >= 0)
);

-- Preserve unknown RPI status if this table was created by an earlier
-- checkpoint that represented missing Binance `nq` as a default zero.
ALTER TABLE binance_microstructure_1s
    ALTER COLUMN fut_rpi_buy_usdt DROP NOT NULL,
    ALTER COLUMN fut_rpi_buy_usdt DROP DEFAULT,
    ALTER COLUMN fut_rpi_sell_usdt DROP NOT NULL,
    ALTER COLUMN fut_rpi_sell_usdt DROP DEFAULT;

CREATE INDEX IF NOT EXISTS binance_microstructure_1s_market_idx
    ON binance_microstructure_1s (market_id, sample_second_ms);

REVOKE ALL ON binance_microstructure_1s FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON binance_microstructure_1s TO price_writer;
GRANT SELECT ON binance_microstructure_1s TO price_reader;


CREATE TABLE IF NOT EXISTS binance_futures_oi_5m_summaries (
    symbol TEXT NOT NULL,

    source_window_start_ms BIGINT NOT NULL,
    source_window_end_ms BIGINT NOT NULL,

    effective_market_id BIGINT NOT NULL REFERENCES market_windows(market_id),

    binance_timestamp_ms BIGINT NOT NULL,

    sum_open_interest NUMERIC(38, 18),
    sum_open_interest_value NUMERIC(38, 18),

    received_ms BIGINT NOT NULL,
    raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (symbol, source_window_start_ms, source_window_end_ms),

    CHECK (source_window_end_ms = source_window_start_ms + 300000),
    CHECK (source_window_start_ms % 300000 = 0)
);

CREATE INDEX IF NOT EXISTS binance_futures_oi_5m_effective_market_idx
    ON binance_futures_oi_5m_summaries (effective_market_id);

-- High-resolution evidence is isolated from the public application schema.
-- The writer may manage partitions only inside this schema; the API reader is
-- intentionally not granted access.
CREATE SCHEMA IF NOT EXISTS raw_capture;
ALTER SCHEMA raw_capture OWNER TO CURRENT_USER;
REVOKE ALL ON SCHEMA raw_capture FROM PUBLIC, price_reader;
GRANT USAGE, CREATE ON SCHEMA raw_capture TO price_writer;

CREATE TABLE IF NOT EXISTS raw_capture.binance_futures_price_trace_100ms (
    bucket_start_ms BIGINT NOT NULL,
    connection_id UUID NOT NULL,

    first_received_wall_ns BIGINT NOT NULL,
    last_received_wall_ns BIGINT NOT NULL,
    first_received_monotonic_ns BIGINT NOT NULL,
    last_received_monotonic_ns BIGINT NOT NULL,

    first_trade_time_ms BIGINT NOT NULL,
    last_trade_time_ms BIGINT NOT NULL,
    first_event_time_ms BIGINT NOT NULL,
    last_event_time_ms BIGINT NOT NULL,

    open_price NUMERIC(38, 18) NOT NULL,
    high_price NUMERIC(38, 18) NOT NULL,
    low_price NUMERIC(38, 18) NOT NULL,
    close_price NUMERIC(38, 18) NOT NULL,

    event_count INTEGER NOT NULL,
    first_agg_trade_id BIGINT NOT NULL,
    last_agg_trade_id BIGINT NOT NULL,

    CHECK (bucket_start_ms >= 0 AND bucket_start_ms % 100 = 0),
    CHECK (first_received_wall_ns > 0),
    CHECK (last_received_wall_ns > 0),
    CHECK (
        first_received_wall_ns >= bucket_start_ms * 1000000
        AND first_received_wall_ns < (bucket_start_ms + 100) * 1000000
    ),
    CHECK (
        last_received_wall_ns >= bucket_start_ms * 1000000
        AND last_received_wall_ns < (bucket_start_ms + 100) * 1000000
    ),
    CHECK (first_received_monotonic_ns > 0),
    CHECK (last_received_monotonic_ns >= first_received_monotonic_ns),
    CHECK (first_trade_time_ms > 0),
    CHECK (last_trade_time_ms > 0),
    CHECK (first_event_time_ms > 0),
    CHECK (last_event_time_ms > 0),
    CHECK (low_price > 0),
    CHECK (high_price >= low_price),
    CHECK (open_price >= low_price AND open_price <= high_price),
    CHECK (close_price >= low_price AND close_price <= high_price),
    CHECK (event_count > 0),
    CHECK (first_agg_trade_id >= 0),
    CHECK (last_agg_trade_id >= 0)
) PARTITION BY RANGE (bucket_start_ms);

ALTER TABLE raw_capture.binance_futures_price_trace_100ms
    OWNER TO price_writer;

CREATE TABLE IF NOT EXISTS raw_capture.chainlink_price_events (
    received_wall_ns BIGINT NOT NULL,
    received_monotonic_ns BIGINT NOT NULL,
    connection_id UUID NOT NULL,
    receive_sequence BIGINT NOT NULL,
    provider_event_ms BIGINT NOT NULL,
    provider_message_ms BIGINT,
    price NUMERIC(38, 18) NOT NULL,

    CHECK (received_wall_ns > 0),
    CHECK (received_monotonic_ns > 0),
    CHECK (receive_sequence > 0),
    CHECK (provider_event_ms > 0),
    CHECK (provider_message_ms IS NULL OR provider_message_ms > 0),
    CHECK (price > 0)
) PARTITION BY RANGE (received_wall_ns);

ALTER TABLE raw_capture.chainlink_price_events
    OWNER TO price_writer;


CREATE TABLE IF NOT EXISTS raw_capture.feed_sessions (
    connection_id UUID PRIMARY KEY,
    source TEXT NOT NULL,
    connected_wall_ns BIGINT NOT NULL,
    connected_monotonic_ns BIGINT NOT NULL,
    ready_wall_ns BIGINT,
    ready_monotonic_ns BIGINT,
    disconnected_wall_ns BIGINT,
    disconnected_monotonic_ns BIGINT,
    close_reason TEXT,
    messages_received_total BIGINT NOT NULL DEFAULT 0,
    messages_accepted_total BIGINT NOT NULL DEFAULT 0,
    parse_errors_total BIGINT NOT NULL DEFAULT 0,
    records_dropped_total BIGINT NOT NULL DEFAULT 0,
    last_receive_sequence BIGINT NOT NULL DEFAULT 0,

    CHECK (source IN (
        'binance_futures_agg_trade',
        'polymarket_chainlink_rtds'
    )),
    CHECK (connected_wall_ns > 0),
    CHECK (connected_monotonic_ns > 0),
    CHECK ((ready_wall_ns IS NULL) = (ready_monotonic_ns IS NULL)),
    CHECK (ready_wall_ns IS NULL OR ready_wall_ns > 0),
    CHECK (ready_monotonic_ns IS NULL OR ready_monotonic_ns >= connected_monotonic_ns),
    CHECK ((disconnected_wall_ns IS NULL) = (disconnected_monotonic_ns IS NULL)),
    CHECK (disconnected_wall_ns IS NULL OR disconnected_wall_ns > 0),
    CHECK (
        disconnected_monotonic_ns IS NULL
        OR disconnected_monotonic_ns >= connected_monotonic_ns
    ),
    CHECK (
        disconnected_monotonic_ns IS NULL
        OR ready_monotonic_ns IS NULL
        OR disconnected_monotonic_ns >= ready_monotonic_ns
    ),
    CHECK (
        close_reason IS NULL
        OR close_reason IN (
            'remote_close',
            'error',
            'proactive_reconnect',
            'cancelled',
            'shutdown'
        )
    ),
    CHECK ((disconnected_wall_ns IS NULL) = (close_reason IS NULL)),
    CHECK (messages_received_total >= 0),
    CHECK (messages_accepted_total >= 0),
    CHECK (parse_errors_total >= 0),
    CHECK (records_dropped_total >= 0),
    CHECK (messages_accepted_total <= messages_received_total),
    CHECK (parse_errors_total <= messages_received_total),
    CHECK (
        messages_accepted_total + parse_errors_total
        <= messages_received_total
    ),
    CHECK (last_receive_sequence = messages_received_total)
);

ALTER TABLE raw_capture.feed_sessions
    OWNER TO price_writer;

-- Preserve session metadata while any retained raw sample still references it.
CREATE INDEX IF NOT EXISTS raw_futures_retention_connection_idx
    ON raw_capture.binance_futures_price_trace_100ms (connection_id);
CREATE INDEX IF NOT EXISTS raw_chainlink_retention_connection_idx
    ON raw_capture.chainlink_price_events (connection_id);

-- Existing installations may still contain this retired table. Index only
-- when already present, for expiry/FK checks; never restore the old pipeline.
DO $$
BEGIN
    IF to_regclass('public.chainlink_twap_shadow_predictions') IS NOT NULL THEN
        CREATE INDEX IF NOT EXISTS chainlink_twap_shadow_retention_market_idx
            ON public.chainlink_twap_shadow_predictions (market_id);
    END IF;
END
$$;

-- Seed the receive-time partition covering deployment and the following one.
-- Runtime maintenance refreshes this pair before later boundaries.
SET ROLE price_writer;

DO $$
DECLARE
    partition_width_ms CONSTANT BIGINT := 21600000;
    current_start_ms BIGINT;
    partition_start_ms BIGINT;
    partition_end_ms BIGINT;
    partition_start_ns BIGINT;
    partition_end_ns BIGINT;
    offset_index INTEGER;
BEGIN
    current_start_ms := (
        (floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT)
        / partition_width_ms
    ) * partition_width_ms;

    FOR offset_index IN 0..1 LOOP
        partition_start_ms := current_start_ms + offset_index * partition_width_ms;
        partition_end_ms := partition_start_ms + partition_width_ms;
        partition_start_ns := partition_start_ms * 1000000;
        partition_end_ns := partition_end_ms * 1000000;

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS raw_capture.%I '
            'PARTITION OF raw_capture.binance_futures_price_trace_100ms '
            'FOR VALUES FROM (%s) TO (%s)',
            'binance_futures_price_trace_100ms_p' || partition_start_ms,
            partition_start_ms,
            partition_end_ms
        );

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS raw_capture.%I '
            'PARTITION OF raw_capture.chainlink_price_events '
            'FOR VALUES FROM (%s) TO (%s)',
            'chainlink_price_events_p' || partition_start_ms,
            partition_start_ns,
            partition_end_ns
        );
    END LOOP;
END
$$;

RESET ROLE;

REVOKE ALL ON ALL TABLES IN SCHEMA raw_capture FROM PUBLIC, price_reader;

INSERT INTO providers (provider_code, display_name)
VALUES ('binance_spot', 'Binance Spot')
ON CONFLICT (provider_code) DO NOTHING;

INSERT INTO instruments (
    provider_id,
    symbol,
    base_asset,
    quote_asset,
    stream_name
)
SELECT
    provider_id,
    'BTCUSDT',
    'BTC',
    'USDT',
    'btcusdt@ticker'
FROM providers
WHERE provider_code = 'binance_spot'
ON CONFLICT (provider_id, symbol) DO NOTHING;

INSERT INTO providers (provider_code, display_name)
VALUES ('polymarket_chainlink_rtds', 'Polymarket RTDS Chainlink BTC/USD')
ON CONFLICT (provider_code) DO NOTHING;

INSERT INTO instruments (
    provider_id,
    symbol,
    base_asset,
    quote_asset,
    stream_name
)
SELECT
    provider_id,
    'BTCUSD',
    'BTC',
    'USD',
    'crypto_prices_chainlink:btc/usd'
FROM providers
WHERE provider_code = 'polymarket_chainlink_rtds'
ON CONFLICT (provider_id, symbol) DO NOTHING;

INSERT INTO providers (provider_code, display_name)
VALUES (
    'polymarket_chainlink_twap_rtds',
    'Polymarket RTDS Chainlink BTC/USD TWAP'
)
ON CONFLICT (provider_code)
DO UPDATE SET display_name = EXCLUDED.display_name;

INSERT INTO instruments (
    provider_id,
    symbol,
    base_asset,
    quote_asset,
    stream_name
)
SELECT
    provider_id,
    'BTCUSD_TWAP_30S',
    'BTC',
    'USD',
    'crypto_prices_twap_thirty:btc/usd'
FROM providers
WHERE provider_code = 'polymarket_chainlink_twap_rtds'
ON CONFLICT (provider_id, symbol) DO NOTHING;

INSERT INTO instruments (
    provider_id,
    symbol,
    base_asset,
    quote_asset,
    stream_name
)
SELECT
    provider_id,
    'BTCUSD_TWAP_60S',
    'BTC',
    'USD',
    'crypto_prices_twap_sixty:btc/usd'
FROM providers
WHERE provider_code = 'polymarket_chainlink_twap_rtds'
ON CONFLICT (provider_id, symbol) DO NOTHING;

INSERT INTO providers (provider_code, display_name)
VALUES ('binance_usdm_perp', 'Binance USD-M Perpetual Futures')
ON CONFLICT (provider_code) DO NOTHING;

INSERT INTO instruments (
    provider_id,
    symbol,
    base_asset,
    quote_asset,
    stream_name
)
SELECT
    provider_id,
    'BTCUSDT',
    'BTC',
    'USDT',
    'binance_usdm_perp:BTCUSDT'
FROM providers
WHERE provider_code = 'binance_usdm_perp'
ON CONFLICT (provider_id, symbol) DO NOTHING;

-- A database reset preserves the login roles but removes database-local
-- privileges. Keep schema.sql sufficient to restore the writer/reader split
-- without relying on an earlier manual bootstrap session.
DO $$
BEGIN
    EXECUTE format(
        'GRANT CONNECT ON DATABASE %I TO price_writer, price_reader',
        current_database()
    );
END
$$;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO price_writer, price_reader;

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC, price_reader;
GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN SCHEMA public TO price_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO price_reader;

-- Override the broad legacy writer grant for immutable prospective evidence.
-- Keep this after that grant so rerunning schema.sql preserves append-only use.
REVOKE UPDATE, DELETE ON polymarket_evidence_payloads FROM price_writer;
REVOKE UPDATE, DELETE ON polymarket_market_observations FROM price_writer;
REVOKE UPDATE, DELETE ON polymarket_quote_observations FROM price_writer;

-- Ghost audit privilege boundary.
-- The operator installing this schema owns the audit table. The collector can
-- append evidence and advance state, but cannot attest to an external export,
-- delete/truncate rows, replace guards, or alter previously frozen inputs.
ALTER TABLE public.ghost_twap_audit OWNER TO CURRENT_USER;
REVOKE ALL ON public.ghost_twap_audit FROM price_writer;
REVOKE INSERT (run_id, decision_id, decision_wall_ns, created_ms, frozen_json,
    frozen_sha256, target_source_timestamps_ms, state_json, state_sha256, version,
    terminal, updated_at, verified_export_sha256, verified_external_location,
    verified_version, verified_frozen_sha256, verified_state_sha256, verified_at),
    UPDATE (run_id, decision_id, decision_wall_ns, created_ms, frozen_json,
    frozen_sha256, target_source_timestamps_ms, state_json, state_sha256, version,
    terminal, updated_at, verified_export_sha256, verified_external_location,
    verified_version, verified_frozen_sha256, verified_state_sha256, verified_at)
    ON public.ghost_twap_audit FROM price_writer;
GRANT SELECT ON public.ghost_twap_audit TO price_writer;
GRANT INSERT (run_id, decision_id, decision_wall_ns, created_ms, frozen_json,
    target_source_timestamps_ms, state_json, version, terminal)
    ON public.ghost_twap_audit TO price_writer;
GRANT UPDATE (state_json, version, terminal) ON public.ghost_twap_audit TO price_writer;
-- Undo the broad ordinary-table grants above for the continuous retention
-- tables. Only the guarded atomic SECURITY DEFINER functions may write them.
REVOKE ALL ON public.ghost_twap_compact,public.ghost_twap_accuracy_hourly,
    public.ghost_twap_retention_state,public.ghost_twap_feed_health FROM PUBLIC,price_writer,price_reader;
GRANT SELECT ON public.ghost_twap_compact,public.ghost_twap_accuracy_hourly,
    public.ghost_twap_retention_state,public.ghost_twap_feed_health TO price_writer,price_reader;
-- End ghost audit privilege boundary.


REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC, price_reader;
GRANT USAGE, SELECT, UPDATE
    ON ALL SEQUENCES IN SCHEMA public TO price_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO price_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO price_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    REVOKE ALL ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO price_writer;
COMMIT;
