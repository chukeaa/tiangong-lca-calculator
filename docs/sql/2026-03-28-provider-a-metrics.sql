-- Provider resolution and A-write monitoring queries
-- Date: 2026-03-28
--
-- Notes:
-- - This file only reads from the existing `lca_snapshot_artifacts.coverage` jsonb payload.
-- - No table / column migration is required.
-- - Newer snapshots may already persist `snapshot_coverage.v2` grouped diagnostics;
--   older snapshots will fall back to v1-style provider decision diagnostics or derived formulas.
--
-- Core derived metrics:
--   provider_usable_pct =
--     (matched_unique_provider + matched_multi_resolved + matched_multi_fallback_equal)
--     / input_edges_total * 100
--
--   a_write_pct =
--     a_input_edges_written / input_edges_total * 100
--
-- Interpretation:
-- - provider_usable_pct tells us how much of the candidate input-edge space can actually
--   be turned into a computable provider decision.
-- - a_write_pct tells us how much of the candidate input-edge space is finally written into A.
-- - When provider_usable_pct and a_write_pct are identical, the current write path is effectively
--   limited by provider resolution.

-- 1) Latest active snapshot metrics
WITH active AS (
  SELECT scope, snapshot_id, activated_at
  FROM public.lca_active_snapshots
  ORDER BY activated_at DESC
  LIMIT 1
)
SELECT a.scope AS active_scope,
       s.id::text AS snapshot_id,
       s.status AS snapshot_status,
       s.provider_matching_rule,
       to_char(a.activated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS activated_at_utc,
       art.process_count,
       art.a_nnz,
       (art.coverage #>> '{matching,input_edges_total}')::numeric AS input_edges_total,
       (art.coverage #>> '{matching,matched_unique_provider}')::numeric AS matched_unique_provider,
       (art.coverage #>> '{matching,matched_multi_resolved}')::numeric AS matched_multi_resolved,
       (art.coverage #>> '{matching,matched_multi_fallback_equal}')::numeric AS matched_multi_fallback_equal,
       (art.coverage #>> '{matching,matched_multi_unresolved}')::numeric AS matched_multi_unresolved,
       (art.coverage #>> '{matching,unmatched_no_provider}')::numeric AS unmatched_no_provider,
       (art.coverage #>> '{matching,a_input_edges_written}')::numeric AS a_input_edges_written,
       coalesce(
         (art.coverage #>> '{matching,a_write_pct}')::numeric,
         round(
           ((art.coverage #>> '{matching,a_input_edges_written}')::numeric
             / nullif((art.coverage #>> '{matching,input_edges_total}')::numeric, 0)) * 100,
           2
         )
       ) AS a_write_pct,
       coalesce(
         (art.coverage #>> '{matching,provider_present_resolved_pct}')::numeric,
         round(
           ((art.coverage #>> '{matching,a_input_edges_written}')::numeric
             / nullif(
                 ((art.coverage #>> '{matching,matched_unique_provider}')::numeric
                + (art.coverage #>> '{matching,matched_multi_provider}')::numeric),
                 0
               )) * 100,
           2
         )
       ) AS provider_present_resolved_pct,
       round(
         (((art.coverage #>> '{matching,matched_unique_provider}')::numeric
          + (art.coverage #>> '{matching,matched_multi_resolved}')::numeric
          + (art.coverage #>> '{matching,matched_multi_fallback_equal}')::numeric)
          / nullif((art.coverage #>> '{matching,input_edges_total}')::numeric, 0)) * 100,
         2
       ) AS provider_usable_pct,
       round(
         ((art.coverage #>> '{matching,matched_multi_fallback_equal}')::numeric
           / nullif((art.coverage #>> '{matching,matched_multi_provider}')::numeric, 0)) * 100,
         2
       ) AS multi_provider_equal_fallback_pct,
       (art.coverage #>> '{matching,any_provider_match_pct}')::numeric AS any_provider_match_pct,
       coalesce(art.coverage #>> '{schema_version}', 'snapshot_coverage.v1') AS coverage_schema_version,
       coalesce(
         art.coverage #> '{matching,candidate_summary,candidate_count_histogram}',
         '{}'::jsonb
       ) AS candidate_count_histogram,
       coalesce(
         art.coverage #> '{matching,resolution_summary,resolved_strategy_counts}',
         art.coverage #> '{matching,provider_decision_diagnostics,resolved_strategy_counts}',
         '{}'::jsonb
       ) AS resolved_strategy_counts,
       coalesce(
         art.coverage #> '{matching,resolution_summary,unresolved_reason_counts}',
         art.coverage #> '{matching,provider_decision_diagnostics,unresolved_reason_counts}',
         '{}'::jsonb
       ) AS unresolved_reason_counts,
       coalesce(
         art.coverage #> '{matching,geography_summary,tier_counts}',
         art.coverage #> '{matching,provider_decision_diagnostics,geography_tier_counts}',
         '{}'::jsonb
       ) AS geography_tier_counts,
       coalesce(
         art.coverage #> '{matching,geography_summary,tier_counts_by_strategy}',
         '{}'::jsonb
       ) AS tier_counts_by_strategy,
       coalesce(
         art.coverage #> '{matching,geography_summary,supply_region_source_counts}',
         art.coverage #> '{matching,provider_decision_diagnostics,supply_region_source_counts}',
         '{}'::jsonb
       ) AS supply_region_source_counts,
       coalesce(
         (art.coverage #>> '{matching,geography_summary,exchange_location_present_count}')::numeric,
         0
       ) AS exchange_location_present_count,
       coalesce(
         art.coverage #> '{matching,geography_summary,requested_location_granularity_counts}',
         '{}'::jsonb
       ) AS requested_location_granularity_counts,
       coalesce(
         art.coverage #> '{matching,volume_weight_summary}',
         '{}'::jsonb
       ) AS volume_weight_summary,
       coalesce(
         (art.coverage #>> '{matching,volume_weight_summary,fallback_to_one_count}')::numeric,
         (art.coverage #>> '{matching,provider_decision_diagnostics,volume_fallback_to_one_count}')::numeric,
         0
       ) AS volume_fallback_to_one_count,
       coalesce(
         art.coverage #> '{matching,gap_summary,unmatched_top_flows}',
         '[]'::jsonb
       ) AS unmatched_top_flows,
       coalesce(
         art.coverage #> '{matching,gap_summary,process_gap_top}',
         '[]'::jsonb
       ) AS process_gap_top,
       (art.coverage #>> '{allocation,allocation_fraction_present_pct}')::numeric AS allocation_fraction_present_pct,
       art.coverage #>> '{singular_risk,risk_level}' AS singular_risk_level,
       coalesce((
         SELECT count(*)
         FROM public.lca_jobs j
         WHERE j.snapshot_id = s.id
           AND j.job_type IN ('solve_one', 'solve_batch', 'solve_all_unit')
           AND j.status = 'completed'
       ), 0) AS completed_solve_jobs
FROM active a
JOIN public.lca_network_snapshots s ON s.id = a.snapshot_id
LEFT JOIN public.lca_snapshot_artifacts art
  ON art.snapshot_id = s.id
 AND art.status = 'ready';

-- 2) Latest successful solve-backed snapshot metrics
WITH latest_solve AS (
  SELECT j.id AS job_id,
         j.job_type,
         j.snapshot_id,
         coalesce(j.finished_at, j.updated_at, j.created_at) AS finished_at
  FROM public.lca_jobs j
  WHERE j.job_type IN ('solve_one', 'solve_batch', 'solve_all_unit')
    AND j.status = 'completed'
  ORDER BY coalesce(j.finished_at, j.updated_at, j.created_at) DESC
  LIMIT 1
)
SELECT ls.job_type,
       ls.job_id::text,
       s.id::text AS snapshot_id,
       to_char(ls.finished_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS finished_at_utc,
       art.process_count,
       art.a_nnz,
       (art.coverage #>> '{matching,input_edges_total}')::numeric AS input_edges_total,
       (art.coverage #>> '{matching,matched_unique_provider}')::numeric AS matched_unique_provider,
       (art.coverage #>> '{matching,matched_multi_resolved}')::numeric AS matched_multi_resolved,
       (art.coverage #>> '{matching,matched_multi_fallback_equal}')::numeric AS matched_multi_fallback_equal,
       (art.coverage #>> '{matching,matched_multi_unresolved}')::numeric AS matched_multi_unresolved,
       (art.coverage #>> '{matching,unmatched_no_provider}')::numeric AS unmatched_no_provider,
       (art.coverage #>> '{matching,a_input_edges_written}')::numeric AS a_input_edges_written,
       coalesce(
         (art.coverage #>> '{matching,a_write_pct}')::numeric,
         round(
           ((art.coverage #>> '{matching,a_input_edges_written}')::numeric
             / nullif((art.coverage #>> '{matching,input_edges_total}')::numeric, 0)) * 100,
           2
         )
       ) AS a_write_pct,
       coalesce(
         (art.coverage #>> '{matching,provider_present_resolved_pct}')::numeric,
         round(
           ((art.coverage #>> '{matching,a_input_edges_written}')::numeric
             / nullif(
                 ((art.coverage #>> '{matching,matched_unique_provider}')::numeric
                + (art.coverage #>> '{matching,matched_multi_provider}')::numeric),
                 0
               )) * 100,
           2
         )
       ) AS provider_present_resolved_pct,
       round(
         (((art.coverage #>> '{matching,matched_unique_provider}')::numeric
          + (art.coverage #>> '{matching,matched_multi_resolved}')::numeric
          + (art.coverage #>> '{matching,matched_multi_fallback_equal}')::numeric)
          / nullif((art.coverage #>> '{matching,input_edges_total}')::numeric, 0)) * 100,
         2
       ) AS provider_usable_pct,
       round(
         ((art.coverage #>> '{matching,matched_multi_fallback_equal}')::numeric
           / nullif((art.coverage #>> '{matching,matched_multi_provider}')::numeric, 0)) * 100,
         2
       ) AS multi_provider_equal_fallback_pct,
       (art.coverage #>> '{matching,any_provider_match_pct}')::numeric AS any_provider_match_pct,
       coalesce(art.coverage #>> '{schema_version}', 'snapshot_coverage.v1') AS coverage_schema_version,
       coalesce(
         art.coverage #> '{matching,candidate_summary,candidate_count_histogram}',
         '{}'::jsonb
       ) AS candidate_count_histogram,
       coalesce(
         art.coverage #> '{matching,resolution_summary,resolved_strategy_counts}',
         art.coverage #> '{matching,provider_decision_diagnostics,resolved_strategy_counts}',
         '{}'::jsonb
       ) AS resolved_strategy_counts,
       coalesce(
         art.coverage #> '{matching,resolution_summary,unresolved_reason_counts}',
         art.coverage #> '{matching,provider_decision_diagnostics,unresolved_reason_counts}',
         '{}'::jsonb
       ) AS unresolved_reason_counts,
       coalesce(
         art.coverage #> '{matching,geography_summary,tier_counts}',
         art.coverage #> '{matching,provider_decision_diagnostics,geography_tier_counts}',
         '{}'::jsonb
       ) AS geography_tier_counts,
       coalesce(
         art.coverage #> '{matching,geography_summary,tier_counts_by_strategy}',
         '{}'::jsonb
       ) AS tier_counts_by_strategy,
       coalesce(
         art.coverage #> '{matching,geography_summary,supply_region_source_counts}',
         art.coverage #> '{matching,provider_decision_diagnostics,supply_region_source_counts}',
         '{}'::jsonb
       ) AS supply_region_source_counts,
       coalesce(
         (art.coverage #>> '{matching,geography_summary,exchange_location_present_count}')::numeric,
         0
       ) AS exchange_location_present_count,
       coalesce(
         art.coverage #> '{matching,geography_summary,requested_location_granularity_counts}',
         '{}'::jsonb
       ) AS requested_location_granularity_counts,
       coalesce(
         art.coverage #> '{matching,volume_weight_summary}',
         '{}'::jsonb
       ) AS volume_weight_summary,
       coalesce(
         (art.coverage #>> '{matching,volume_weight_summary,fallback_to_one_count}')::numeric,
         (art.coverage #>> '{matching,provider_decision_diagnostics,volume_fallback_to_one_count}')::numeric,
         0
       ) AS volume_fallback_to_one_count,
       coalesce(
         art.coverage #> '{matching,gap_summary,unmatched_top_flows}',
         '[]'::jsonb
       ) AS unmatched_top_flows,
       coalesce(
         art.coverage #> '{matching,gap_summary,process_gap_top}',
         '[]'::jsonb
       ) AS process_gap_top,
       (art.coverage #>> '{allocation,allocation_fraction_present_pct}')::numeric AS allocation_fraction_present_pct,
       art.coverage #>> '{singular_risk,risk_level}' AS singular_risk_level
FROM latest_solve ls
JOIN public.lca_network_snapshots s ON s.id = ls.snapshot_id
LEFT JOIN public.lca_snapshot_artifacts art
  ON art.snapshot_id = s.id
 AND art.status = 'ready';

-- 3) Recent solve-backed trend (one row per snapshot)
WITH latest_completed_per_snapshot AS (
  SELECT DISTINCT ON (j.snapshot_id)
         j.snapshot_id,
         coalesce(j.finished_at, j.updated_at, j.created_at) AS finished_at
  FROM public.lca_jobs j
  WHERE j.job_type IN ('solve_one', 'solve_batch', 'solve_all_unit')
    AND j.status = 'completed'
  ORDER BY j.snapshot_id, coalesce(j.finished_at, j.updated_at, j.created_at) DESC
)
SELECT s.id::text AS snapshot_id,
       to_char(l.finished_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS latest_solve_at_utc,
       art.process_count,
       art.a_nnz,
       (art.coverage #>> '{matching,input_edges_total}')::numeric AS input_edges_total,
       (art.coverage #>> '{matching,a_input_edges_written}')::numeric AS a_input_edges_written,
       coalesce(
         (art.coverage #>> '{matching,a_write_pct}')::numeric,
         round(
           ((art.coverage #>> '{matching,a_input_edges_written}')::numeric
             / nullif((art.coverage #>> '{matching,input_edges_total}')::numeric, 0)) * 100,
           2
         )
       ) AS a_write_pct,
       coalesce(
         (art.coverage #>> '{matching,provider_present_resolved_pct}')::numeric,
         round(
           ((art.coverage #>> '{matching,a_input_edges_written}')::numeric
             / nullif(
                 ((art.coverage #>> '{matching,matched_unique_provider}')::numeric
                + (art.coverage #>> '{matching,matched_multi_provider}')::numeric),
                 0
               )) * 100,
           2
         )
       ) AS provider_present_resolved_pct,
       round(
         (((art.coverage #>> '{matching,matched_unique_provider}')::numeric
          + (art.coverage #>> '{matching,matched_multi_resolved}')::numeric
          + (art.coverage #>> '{matching,matched_multi_fallback_equal}')::numeric)
          / nullif((art.coverage #>> '{matching,input_edges_total}')::numeric, 0)) * 100,
         2
       ) AS provider_usable_pct,
       round(
         ((art.coverage #>> '{matching,matched_multi_fallback_equal}')::numeric
           / nullif((art.coverage #>> '{matching,matched_multi_provider}')::numeric, 0)) * 100,
         2
       ) AS multi_provider_equal_fallback_pct,
       (art.coverage #>> '{matching,matched_multi_unresolved}')::numeric AS matched_multi_unresolved,
       (art.coverage #>> '{matching,unmatched_no_provider}')::numeric AS unmatched_no_provider,
       coalesce(
         art.coverage #> '{matching,resolution_summary,resolved_strategy_counts}',
         art.coverage #> '{matching,provider_decision_diagnostics,resolved_strategy_counts}',
         '{}'::jsonb
       ) AS resolved_strategy_counts,
       coalesce(
         art.coverage #> '{matching,resolution_summary,unresolved_reason_counts}',
         art.coverage #> '{matching,provider_decision_diagnostics,unresolved_reason_counts}',
         '{}'::jsonb
       ) AS unresolved_reason_counts,
       coalesce(
         art.coverage #> '{matching,geography_summary,tier_counts}',
         art.coverage #> '{matching,provider_decision_diagnostics,geography_tier_counts}',
         '{}'::jsonb
       ) AS geography_tier_counts,
       coalesce(
         art.coverage #> '{matching,volume_weight_summary}',
         '{}'::jsonb
       ) AS volume_weight_summary,
       (art.coverage #>> '{allocation,allocation_fraction_present_pct}')::numeric AS allocation_fraction_present_pct
FROM latest_completed_per_snapshot l
JOIN public.lca_network_snapshots s ON s.id = l.snapshot_id
JOIN public.lca_snapshot_artifacts art
  ON art.snapshot_id = s.id
 AND art.status = 'ready'
ORDER BY l.finished_at DESC
LIMIT 8;

-- 4) Compare the latest active snapshot against the latest successful solve-backed snapshot
WITH active AS (
  SELECT snapshot_id
  FROM public.lca_active_snapshots
  ORDER BY activated_at DESC
  LIMIT 1
),
latest_solve AS (
  SELECT snapshot_id
  FROM public.lca_jobs
  WHERE job_type IN ('solve_one', 'solve_batch', 'solve_all_unit')
    AND status = 'completed'
  ORDER BY coalesce(finished_at, updated_at, created_at) DESC
  LIMIT 1
),
targets AS (
  SELECT 'latest_active' AS target_kind, snapshot_id FROM active
  UNION ALL
  SELECT 'latest_successful_solve' AS target_kind, snapshot_id FROM latest_solve
)
SELECT t.target_kind,
       s.id::text AS snapshot_id,
       art.process_count,
       art.a_nnz,
       (art.coverage #>> '{matching,input_edges_total}')::numeric AS input_edges_total,
       (art.coverage #>> '{matching,a_input_edges_written}')::numeric AS a_input_edges_written,
       coalesce(
         (art.coverage #>> '{matching,a_write_pct}')::numeric,
         round(
           ((art.coverage #>> '{matching,a_input_edges_written}')::numeric
             / nullif((art.coverage #>> '{matching,input_edges_total}')::numeric, 0)) * 100,
           2
         )
       ) AS a_write_pct,
       coalesce(
         (art.coverage #>> '{matching,provider_present_resolved_pct}')::numeric,
         round(
           ((art.coverage #>> '{matching,a_input_edges_written}')::numeric
             / nullif(
                 ((art.coverage #>> '{matching,matched_unique_provider}')::numeric
                + (art.coverage #>> '{matching,matched_multi_provider}')::numeric),
                 0
               )) * 100,
           2
         )
       ) AS provider_present_resolved_pct,
       round(
         (((art.coverage #>> '{matching,matched_unique_provider}')::numeric
          + (art.coverage #>> '{matching,matched_multi_resolved}')::numeric
          + (art.coverage #>> '{matching,matched_multi_fallback_equal}')::numeric)
          / nullif((art.coverage #>> '{matching,input_edges_total}')::numeric, 0)) * 100,
         2
       ) AS provider_usable_pct,
       round(
         ((art.coverage #>> '{matching,matched_multi_fallback_equal}')::numeric
           / nullif((art.coverage #>> '{matching,matched_multi_provider}')::numeric, 0)) * 100,
         2
       ) AS multi_provider_equal_fallback_pct,
       (art.coverage #>> '{matching,matched_multi_unresolved}')::numeric AS matched_multi_unresolved,
       (art.coverage #>> '{matching,unmatched_no_provider}')::numeric AS unmatched_no_provider,
       coalesce(
         art.coverage #> '{matching,resolution_summary,resolved_strategy_counts}',
         art.coverage #> '{matching,provider_decision_diagnostics,resolved_strategy_counts}',
         '{}'::jsonb
       ) AS resolved_strategy_counts,
       coalesce(
         art.coverage #> '{matching,resolution_summary,unresolved_reason_counts}',
         art.coverage #> '{matching,provider_decision_diagnostics,unresolved_reason_counts}',
         '{}'::jsonb
       ) AS unresolved_reason_counts
FROM targets t
JOIN public.lca_network_snapshots s ON s.id = t.snapshot_id
LEFT JOIN public.lca_snapshot_artifacts art
  ON art.snapshot_id = s.id
 AND art.status = 'ready'
ORDER BY t.target_kind;

-- 5) Latest active snapshot unresolved reason breakdown
WITH active AS (
  SELECT snapshot_id
  FROM public.lca_active_snapshots
  ORDER BY activated_at DESC
  LIMIT 1
)
SELECT s.id::text AS snapshot_id,
       reason.key AS unresolved_reason,
       reason.value::numeric AS unresolved_count
FROM active a
JOIN public.lca_network_snapshots s ON s.id = a.snapshot_id
JOIN public.lca_snapshot_artifacts art
  ON art.snapshot_id = s.id
 AND art.status = 'ready'
LEFT JOIN LATERAL jsonb_each_text(
  coalesce(
    art.coverage #> '{matching,resolution_summary,unresolved_reason_counts}',
    art.coverage #> '{matching,provider_decision_diagnostics,unresolved_reason_counts}',
    '{}'::jsonb
  )
) AS reason(key, value) ON true
ORDER BY unresolved_count DESC NULLS LAST, unresolved_reason;

-- 6) Latest active snapshot resolved strategy breakdown
WITH active AS (
  SELECT snapshot_id
  FROM public.lca_active_snapshots
  ORDER BY activated_at DESC
  LIMIT 1
)
SELECT s.id::text AS snapshot_id,
       strategy.key AS resolved_strategy,
       strategy.value::numeric AS resolved_count
FROM active a
JOIN public.lca_network_snapshots s ON s.id = a.snapshot_id
JOIN public.lca_snapshot_artifacts art
  ON art.snapshot_id = s.id
 AND art.status = 'ready'
LEFT JOIN LATERAL jsonb_each_text(
  coalesce(
    art.coverage #> '{matching,resolution_summary,resolved_strategy_counts}',
    art.coverage #> '{matching,provider_decision_diagnostics,resolved_strategy_counts}',
    '{}'::jsonb
  )
) AS strategy(key, value) ON true
ORDER BY resolved_count DESC NULLS LAST, resolved_strategy;

-- 7) Latest active snapshot geography tier breakdown by resolution strategy
WITH active AS (
  SELECT snapshot_id
  FROM public.lca_active_snapshots
  ORDER BY activated_at DESC
  LIMIT 1
)
SELECT s.id::text AS snapshot_id,
       strategy.key AS resolved_strategy,
       tier.key AS geography_tier,
       tier.value::numeric AS edge_count
FROM active a
JOIN public.lca_network_snapshots s ON s.id = a.snapshot_id
JOIN public.lca_snapshot_artifacts art
  ON art.snapshot_id = s.id
 AND art.status = 'ready'
LEFT JOIN LATERAL jsonb_each(
  coalesce(
    art.coverage #> '{matching,geography_summary,tier_counts_by_strategy}',
    '{}'::jsonb
  )
) AS strategy(key, value) ON true
LEFT JOIN LATERAL jsonb_each_text(strategy.value) AS tier(key, value) ON true
ORDER BY edge_count DESC NULLS LAST, resolved_strategy, geography_tier;

-- 8) Latest active snapshot volume-weight data quality summary
WITH active AS (
  SELECT snapshot_id
  FROM public.lca_active_snapshots
  ORDER BY activated_at DESC
  LIMIT 1
)
SELECT s.id::text AS snapshot_id,
       coalesce((art.coverage #>> '{matching,volume_weight_summary,candidate_total}')::numeric, 0) AS candidate_total,
       coalesce((art.coverage #>> '{matching,volume_weight_summary,valid_volume_count}')::numeric, 0) AS valid_volume_count,
       coalesce(
         (art.coverage #>> '{matching,volume_weight_summary,fallback_to_one_count}')::numeric,
         (art.coverage #>> '{matching,provider_decision_diagnostics,volume_fallback_to_one_count}')::numeric,
         0
       ) AS fallback_to_one_count,
       coalesce((art.coverage #>> '{matching,volume_weight_summary,decisions_total}')::numeric, 0) AS decisions_total,
       coalesce((art.coverage #>> '{matching,volume_weight_summary,decisions_all_valid_count}')::numeric, 0) AS decisions_all_valid_count,
       coalesce((art.coverage #>> '{matching,volume_weight_summary,decisions_partial_missing_count}')::numeric, 0) AS decisions_partial_missing_count,
       coalesce((art.coverage #>> '{matching,volume_weight_summary,decisions_all_missing_count}')::numeric, 0) AS decisions_all_missing_count
FROM active a
JOIN public.lca_network_snapshots s ON s.id = a.snapshot_id
JOIN public.lca_snapshot_artifacts art
  ON art.snapshot_id = s.id
 AND art.status = 'ready';

-- 9) Latest active snapshot top no-provider flow gaps
WITH active AS (
  SELECT snapshot_id
  FROM public.lca_active_snapshots
  ORDER BY activated_at DESC
  LIMIT 1
)
SELECT s.id::text AS snapshot_id,
       gap.value->>'flow_id' AS flow_id,
       gap.value->>'flow_name' AS flow_name,
       (gap.value->>'count')::numeric AS unmatched_count
FROM active a
JOIN public.lca_network_snapshots s ON s.id = a.snapshot_id
JOIN public.lca_snapshot_artifacts art
  ON art.snapshot_id = s.id
 AND art.status = 'ready'
LEFT JOIN LATERAL jsonb_array_elements(
  coalesce(
    art.coverage #> '{matching,gap_summary,unmatched_top_flows}',
    '[]'::jsonb
  )
) AS gap(value) ON true
ORDER BY unmatched_count DESC NULLS LAST, flow_id;

-- 10) Latest active snapshot top no-provider process gaps
WITH active AS (
  SELECT snapshot_id
  FROM public.lca_active_snapshots
  ORDER BY activated_at DESC
  LIMIT 1
)
SELECT s.id::text AS snapshot_id,
       gap.value->>'process_id' AS process_id,
       gap.value->>'process_name' AS process_name,
       (gap.value->>'input_edges_total')::numeric AS input_edges_total,
       (gap.value->>'unmatched_no_provider')::numeric AS unmatched_no_provider,
       (gap.value->>'a_write_pct')::numeric AS a_write_pct
FROM active a
JOIN public.lca_network_snapshots s ON s.id = a.snapshot_id
JOIN public.lca_snapshot_artifacts art
  ON art.snapshot_id = s.id
 AND art.status = 'ready'
LEFT JOIN LATERAL jsonb_array_elements(
  coalesce(
    art.coverage #> '{matching,gap_summary,process_gap_top}',
    '[]'::jsonb
  )
) AS gap(value) ON true
ORDER BY unmatched_no_provider DESC NULLS LAST, input_edges_total DESC NULLS LAST, process_id;
