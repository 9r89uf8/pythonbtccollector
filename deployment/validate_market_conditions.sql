-- Run in a separate psql invocation after schema.sql commits and services resume.
-- The NOT VALID check already enforces new inserts/updates. This scan validates
-- retained rows with SHARE UPDATE EXCLUSIVE, which permits ordinary writes.
-- Apply bounded lock_timeout and statement_timeout to this invocation.
ALTER TABLE public.settlement_audit
    VALIDATE CONSTRAINT settlement_audit_observation_window_check;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.settlement_audit'::regclass
          AND conname='settlement_audit_observation_window_check'
          AND convalidated
          AND pg_get_constraintdef(oid) LIKE '%historical-market-conditions-v1%'
    ) THEN
        RAISE EXCEPTION 'market conditions schedule constraint is not installed and validated';
    END IF;
END $$;
