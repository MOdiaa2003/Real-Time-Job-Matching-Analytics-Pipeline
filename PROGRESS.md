# PROGRESS.md — build log (autonomous run)

Started: 2026-10-22 (Stage 3 onwards)
Setup context: Stages 1 (infra docker-compose/.env/database/monitoring) and 2 (connectors/*.json, schemas/*.avsc, register_connectors.ps1) already completed and commit-worthy.

## Stage 3 — Generators

2026-10-22T00:00 — Stage 3 started. Plan:
* recruitment_lifecycle_generator.py: add CLI flags + CONFIG rate knobs (job_close_rate, job_reopen_rate, profile_update_rate, app_status_transition_rate) with default 0 = unchanged behavior.
* NEW Realtime_Pipline/fault_injection.py (duplicate/malformed/late/burst helpers + argparse for on-demand fault injection).
* NEW Realtime_Pipline/application_event_producer.py (Avro+SR idempotent producer for application_submitted_events).

(appended entries in reverse-chronological-ish order as work completes)

2026-10-22T00:35 — Stage 3 code complete:
- recruitment_lifecycle_generator.py: CONFIG rate keys (job_close_rate, job_reopen_rate, profile_update_rate, app_status_transition_rate, duration_seconds), env overrides (JOB_CLOSE_RATE etc.), CLI parse_cli_overrides() (--ops/--duration/--job-close-rate/--job-reopen-rate/--profile-update-rate/--app-status-transition-rate), update_job close branch now uses close_prob=0.15+job_close_rate (capped 0.9), new LifecycleEngine.reopen_job() (closed->open, stage 0, 60s warming), extended APP_TRANSITIONS chain (viewed->interview->offer->accepted/rejected w/ final_outcome) ONLY when app_status_transition_rate>0, choose_operation gains reopen + configurable candidate-update weight. Defaults: all 0 -> byte-for-byte same behavior as before (weights unchanged).
- Realtime_Pipline/fault_injection.py NEW: make_event/make_malformed_payloads (non_avro_bytes, wrong_header, valid_schema_wrong_types)/burst_events/cli_flags.
- Realtime_Pipline/application_event_producer.py NEW: confluent_kafka AvroSerializer (schemas/application_submitted_event.avsc), SchemaRegistryClient, idempotent producer (enable.idempotence=True, acks=all, lz4), key=job_id, flags --rate/--duration/--duplicate-rate/--malformed-rate/--late-event-rate/--burst + pools.
- Verified statically: py_compile OK for all touched files; field-set of make_event matches avsc; avsc JSON parses.

DECISIONS:
- No generate_data.py exists in repo (recruitment_lifecycle_generator.py IS the generator and is ALREADY pure-Postgres); documented in README later, no duplicate generator created.
- reopen keeps 60s warming (reuses warming_jobs) to not reintroduce CDC race unresolved_rows.

OPEN: runtime verification of generators vs live Postgres (next after stack starts in Stage 4 infra bring-up).
