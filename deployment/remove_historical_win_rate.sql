-- Permanent, owner-authorized removal of Historical win rate data.
-- Deploy the code without this feature and stop its old workers before running.
-- No CASCADE: an unexpected external dependency must abort the transaction.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

DROP TABLE IF EXISTS public.settlement_history_daily;
DROP TABLE IF EXISTS public.settlement_history_markets;
DROP TABLE IF EXISTS public.settlement_evaluation_reports;
DROP TABLE IF EXISTS public.settlement_market_evaluation;
DROP TABLE IF EXISTS public.settlement_audit;
DROP FUNCTION IF EXISTS public.settlement_history_guard();
DROP FUNCTION IF EXISTS public.settlement_summary_guard();
DROP FUNCTION IF EXISTS public.settlement_audit_guard();

COMMIT;
