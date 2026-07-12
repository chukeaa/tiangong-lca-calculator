use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::calculation_evidence::LcaCalculationEvidence;
use crate::contribution_path::ContributionPathOptions;
use crate::graph_types::RequestRootProcess;

/// Queue payload for worker jobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JobPayload {
    /// Build and cache factorization.
    PrepareFactorization {
        /// `jobs.id`
        job_id: Uuid,
        /// `lca_network_snapshots.id`
        #[serde(alias = "model_version")]
        snapshot_id: Uuid,
        /// Numeric print level.
        #[serde(default)]
        print_level: Option<f64>,
    },
    /// Solve one RHS with cached factorization.
    SolveOne {
        /// `jobs.id`
        job_id: Uuid,
        /// `lca_network_snapshots.id`
        #[serde(alias = "model_version")]
        snapshot_id: Uuid,
        /// Demand vector y.
        rhs: Vec<f64>,
        /// Output options.
        #[serde(default)]
        solve: SolveOptionsPayload,
        /// Numeric print level.
        #[serde(default)]
        print_level: Option<f64>,
        /// Exact method/factor and coverage binding for versioned scoped snapshots.
        #[serde(default)]
        calculation_evidence_binding: Option<LcaCalculationEvidence>,
    },
    /// Solve multiple RHS vectors.
    SolveBatch {
        /// `jobs.id`
        job_id: Uuid,
        /// `lca_network_snapshots.id`
        #[serde(alias = "model_version")]
        snapshot_id: Uuid,
        /// Demand matrix Y as row-major list of vectors.
        rhs_batch: Vec<Vec<f64>>,
        /// Output options.
        #[serde(default)]
        solve: SolveOptionsPayload,
        /// Numeric print level.
        #[serde(default)]
        print_level: Option<f64>,
        /// Exact method/factor and coverage binding for versioned scoped snapshots.
        #[serde(default)]
        calculation_evidence_binding: Option<LcaCalculationEvidence>,
    },
    /// Solve unit demand for every process in current snapshot.
    SolveAllUnit {
        /// `jobs.id`
        job_id: Uuid,
        /// `lca_network_snapshots.id`
        #[serde(alias = "model_version")]
        snapshot_id: Uuid,
        /// Output options.
        ///
        /// For `solve_all_unit`, worker enforces `return_h=true` and `return_x/return_g=false`
        /// to avoid oversized artifacts.
        #[serde(default)]
        solve: Option<SolveOptionsPayload>,
        /// Batch size for internal `solve_batch` chunks.
        #[serde(default)]
        unit_batch_size: Option<usize>,
        /// Numeric print level.
        #[serde(default)]
        print_level: Option<f64>,
        /// Exact method/factor and coverage binding for versioned scoped snapshots.
        #[serde(default)]
        calculation_evidence_binding: Option<LcaCalculationEvidence>,
    },
    /// Analyze one process + one impact into a contribution path result.
    AnalyzeContributionPath {
        /// `jobs.id`
        job_id: Uuid,
        /// `lca_network_snapshots.id`
        #[serde(alias = "model_version")]
        snapshot_id: Uuid,
        /// Root process business id.
        process_id: Uuid,
        /// Root process index inside snapshot matrices.
        process_index: i32,
        /// Target impact business id.
        impact_id: Uuid,
        /// Target impact index inside `C` and `h`.
        impact_index: i32,
        /// Root demand amount.
        #[serde(default = "default_amount")]
        amount: f64,
        /// Traversal options.
        #[serde(default)]
        options: ContributionPathOptions,
        /// Numeric print level.
        #[serde(default)]
        print_level: Option<f64>,
        /// Exact method/factor and coverage binding for versioned scoped snapshots.
        #[serde(default)]
        calculation_evidence_binding: Option<LcaCalculationEvidence>,
    },
    /// Mark cached factorization stale.
    InvalidateFactorization {
        /// `jobs.id`
        job_id: Uuid,
        /// `lca_network_snapshots.id`
        #[serde(alias = "model_version")]
        snapshot_id: Uuid,
    },
    /// Rebuild factorization immediately.
    RebuildFactorization {
        /// `jobs.id`
        job_id: Uuid,
        /// `lca_network_snapshots.id`
        #[serde(alias = "model_version")]
        snapshot_id: Uuid,
        /// Numeric print level.
        #[serde(default)]
        print_level: Option<f64>,
    },
    /// Build one snapshot artifact for later solve jobs.
    BuildSnapshot {
        /// `jobs.id`
        job_id: Uuid,
        /// Requested snapshot id to persist.
        snapshot_id: Uuid,
        /// Active scope pointer to update after build.
        #[serde(default)]
        scope: Option<String>,
        /// Whether all process states are requested.
        #[serde(default)]
        all_states: Option<bool>,
        /// Process state filter, e.g. `100` or `100,200`.
        #[serde(default)]
        process_states: Option<String>,
        /// Optional `user_id` inclusion.
        #[serde(default)]
        include_user_id: Option<Uuid>,
        /// Owner-only state filter for the versioned private-incubation scope.
        #[serde(default)]
        include_user_state_codes: Option<String>,
        /// Require owner drafts to have no team assignment.
        #[serde(default)]
        include_user_unassigned_only: Option<bool>,
        /// Require owner drafts to have no active review assignment.
        #[serde(default)]
        include_user_review_free_only: Option<bool>,
        /// Named versioned data scope.
        #[serde(default)]
        data_scope: Option<String>,
        /// Frozen visibility predicate manifest.
        #[serde(default)]
        scope_manifest: Option<Value>,
        /// Canonical SHA-256 of `scope_manifest`.
        #[serde(default)]
        scope_manifest_sha256: Option<String>,
        /// Required database LCIA method/factor source contract.
        #[serde(default)]
        lcia_method_factor_source: Option<Value>,
        /// Required factor-coverage evidence contract.
        #[serde(default)]
        lcia_factor_coverage_contract: Option<Value>,
        /// Explicit request roots (`<process_id, version>`) for request-scoped graph builds.
        #[serde(default)]
        request_roots: Option<Vec<RequestRootProcess>>,
        /// Optional provider matching rule.
        #[serde(default)]
        provider_rule: Option<String>,
        /// Optional quantitative reference normalization mode.
        #[serde(default)]
        reference_normalization_mode: Option<String>,
        /// Optional allocation fraction mode.
        #[serde(default)]
        allocation_fraction_mode: Option<String>,
        /// Optional `process_limit`.
        #[serde(default)]
        process_limit: Option<i32>,
        /// Optional self-loop cutoff.
        #[serde(default)]
        self_loop_cutoff: Option<f64>,
        /// Optional near-singular epsilon.
        #[serde(default)]
        singular_eps: Option<f64>,
        /// Optional LCIA method id.
        #[serde(default)]
        method_id: Option<Uuid>,
        /// Optional LCIA method version.
        #[serde(default)]
        method_version: Option<String>,
        /// Disable LCIA factors.
        #[serde(default)]
        no_lcia: Option<bool>,
    },
    /// Build an immutable published-data LCIA result package.
    LciaResultPackageBuild {
        /// Build id generated by `cmd_lcia_result_build_request`.
        build_id: Uuid,
        /// User that requested the package build.
        requested_by: Uuid,
        /// Optional display name supplied by the manager request.
        #[serde(default)]
        name: Option<String>,
        /// `subset` or `global_eligible`.
        coverage_mode: String,
        /// Published-only predicate used to resolve the package inputs.
        #[serde(default)]
        input_status_filter: Option<Value>,
        /// Eligibility definition captured by the database request RPC.
        #[serde(default)]
        eligibility_definition: Option<Value>,
        /// Total current eligible input count at request time.
        eligible_input_count: i32,
        /// Number of inputs included in this package build.
        included_input_count: i32,
        /// Stable hash over the package input manifest.
        input_manifest_hash: String,
        /// Published-only process input manifest.
        input_manifest: Value,
        /// LCIA method metadata captured at request time.
        #[serde(default)]
        lcia_method_set: Value,
        /// Optional default impact category selected for preview/public display.
        #[serde(default)]
        default_impact_category: Option<String>,
        /// Post-processing manifest, no-op for the MVP package build.
        #[serde(default)]
        postprocess_manifest: Option<Value>,
    },
}

fn default_amount() -> f64 {
    1.0
}

/// Solve output flags from payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SolveOptionsPayload {
    /// Return x.
    pub return_x: bool,
    /// Return g.
    pub return_g: bool,
    /// Return h.
    pub return_h: bool,
}

impl Default for SolveOptionsPayload {
    fn default() -> Self {
        Self {
            return_x: true,
            return_g: true,
            return_h: true,
        }
    }
}

/// Internal HTTP solve body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolveHttpBody {
    /// Single RHS.
    pub rhs: Option<Vec<f64>>,
    /// Optional batch RHS.
    pub rhs_batch: Option<Vec<Vec<f64>>>,
    /// Solve flags.
    #[serde(default)]
    pub solve: SolveOptionsPayload,
    /// Numeric print level.
    #[serde(default)]
    pub print_level: Option<f64>,
}

/// Internal HTTP prepare body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareHttpBody {
    /// Numeric print level.
    #[serde(default)]
    pub print_level: Option<f64>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{JobPayload, SolveOptionsPayload};

    #[test]
    fn deserialize_prepare_payload() {
        let payload = json!({
            "type": "prepare_factorization",
            "job_id": Uuid::nil(),
            "snapshot_id": Uuid::nil(),
            "print_level": 0.0
        });

        let parsed: JobPayload = serde_json::from_value(payload).expect("parse payload");
        assert!(matches!(parsed, JobPayload::PrepareFactorization { .. }));
    }

    #[test]
    fn deserialize_prepare_payload_with_model_version_alias() {
        let payload = json!({
            "type": "prepare_factorization",
            "job_id": Uuid::nil(),
            "model_version": Uuid::nil()
        });

        let parsed: JobPayload = serde_json::from_value(payload).expect("parse payload");
        assert!(matches!(parsed, JobPayload::PrepareFactorization { .. }));
    }

    #[test]
    fn deserialize_solve_all_unit_payload_defaults() {
        let payload = json!({
            "type": "solve_all_unit",
            "job_id": Uuid::nil(),
            "snapshot_id": Uuid::nil()
        });
        let parsed: JobPayload = serde_json::from_value(payload).expect("parse payload");
        match parsed {
            JobPayload::SolveAllUnit {
                unit_batch_size,
                solve,
                ..
            } => {
                assert!(unit_batch_size.is_none());
                assert!(solve.is_none());
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn deserialize_solve_all_unit_payload_with_options() {
        let payload = json!({
            "type": "solve_all_unit",
            "job_id": Uuid::nil(),
            "snapshot_id": Uuid::nil(),
            "unit_batch_size": 256,
            "solve": {
                "return_x": false,
                "return_g": false,
                "return_h": true
            }
        });
        let parsed: JobPayload = serde_json::from_value(payload).expect("parse payload");
        match parsed {
            JobPayload::SolveAllUnit {
                unit_batch_size,
                solve,
                ..
            } => {
                assert_eq!(unit_batch_size, Some(256));
                assert_eq!(
                    solve,
                    Some(SolveOptionsPayload {
                        return_x: false,
                        return_g: false,
                        return_h: true,
                    })
                );
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn deserialize_contribution_path_payload_defaults() {
        let process_id = Uuid::new_v4();
        let impact_id = Uuid::new_v4();
        let payload = json!({
            "type": "analyze_contribution_path",
            "job_id": Uuid::nil(),
            "snapshot_id": Uuid::nil(),
            "process_id": process_id,
            "process_index": 12,
            "impact_id": impact_id,
            "impact_index": 3
        });

        let parsed: JobPayload = serde_json::from_value(payload).expect("parse payload");
        match parsed {
            JobPayload::AnalyzeContributionPath {
                process_id: parsed_process_id,
                process_index,
                impact_id: parsed_impact_id,
                impact_index,
                amount,
                options,
                ..
            } => {
                assert_eq!(parsed_process_id, process_id);
                assert_eq!(process_index, 12);
                assert_eq!(parsed_impact_id, impact_id);
                assert_eq!(impact_index, 3);
                assert!((amount - 1.0).abs() < f64::EPSILON);
                assert_eq!(options, super::ContributionPathOptions::default());
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }
}
