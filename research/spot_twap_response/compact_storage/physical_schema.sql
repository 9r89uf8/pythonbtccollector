BEGIN READ ONLY;
SET LOCAL statement_timeout='5s';
SET LOCAL lock_timeout='1s';
SELECT json_build_object(
  'database',current_database(),
  'observed_during_run',true,
  'relations',(SELECT json_agg(json_build_object(
    'name',c.relname,'persistence',c.relpersistence,
    'options',c.reloptions,'toast_options',t.reloptions) ORDER BY c.relname)
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    LEFT JOIN pg_class t ON t.oid=c.reltoastrelid
    WHERE n.nspname='ghost_compact_probe' AND c.relkind='r'),
  'indexes',(SELECT json_agg(json_build_object(
    'table',tablename,'name',indexname,'definition',indexdef) ORDER BY indexname)
    FROM pg_indexes WHERE schemaname='ghost_compact_probe')
);
COMMIT;
